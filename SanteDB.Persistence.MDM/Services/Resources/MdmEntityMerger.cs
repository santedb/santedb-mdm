/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
 * Portions Copyright (C) 2015-2018 Mohawk College of Applied Arts and Technology
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. You may 
 * obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations under 
 * the License.
 * 
 * User: fyfej
 * Date: 2023-3-10
 */
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Attributes;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// An MDM merger that operates on Entities
    /// </summary>
    /// <remarks>This class exists to allow callers to interact with the operations in the underlying infrastructure.</remarks>
    public class MdmEntityMerger<TEntity> : MdmResourceMerger<TEntity>, IReportProgressChanged, IDisposable
        where TEntity : Entity, new()
    {
        private class BackgroundMatchContext : IDisposable
        {

            private readonly ConcurrentStack<TEntity> m_entityStack = new ConcurrentStack<TEntity>();
            private readonly ManualResetEventSlim m_processEvent = new ManualResetEventSlim(false);
            private readonly ManualResetEventSlim m_loadEvent = new ManualResetEventSlim(true);
            private readonly int m_maxWorkers;
            private Exception m_haltException;
            private int m_loadedRecords = 0;
            private int m_availableWorkers;
            private int m_recordsProcessed = 0;
            private bool m_isRunning = true;

            /// <summary>
            /// True if the background processors should be executing
            /// </summary>
            public bool IsRunning => this.m_isRunning;

            /// <summary>
            /// Records processed
            /// </summary>
            public int RecordsProcessed => this.m_recordsProcessed;

            /// <summary>
            /// Create new background matching context
            /// </summary>
            public BackgroundMatchContext(int maxWorkers)
            {
                this.m_maxWorkers = this.m_availableWorkers = maxWorkers;
            }

            /// <summary>
            /// Halt processing 
            /// </summary>
            public void Halt(Exception e)
            {
                this.m_haltException = e;
            }

            /// <summary>
            /// Queue a loaded record
            /// </summary>
            public void QueueLoadedRecord(TEntity record)
            {
                // Main thread
                if (this.m_haltException != null)
                {
                    throw this.m_haltException;
                }

                if (this.m_loadEvent.Wait(1000))
                {  // ensure that we are allowed to add or wait for avialable worker
                    this.m_entityStack.Push(record);
                    if (this.m_loadedRecords++ % 16 == 0)
                    {
                        this.m_processEvent.Set(); // Signal the processing threads that they may process
                    }
                }
            }

            /// <summary>
            /// De-queue a loaded record
            /// </summary>
            public int DeQueueLoadedRecords(TEntity[] records)
            {
                if (this.m_processEvent.Wait(1000))
                {
                    var retVal = this.m_entityStack.TryPopRange(records);
                    if (retVal > 0)
                    {
                        if (Interlocked.Decrement(ref this.m_availableWorkers) == 0)
                        {
                            this.m_loadEvent.Reset();
                        }
                    }

                    return retVal;
                }
                else
                {
                    return 0;
                }
            }

            /// <summary>
            /// Release a worker
            /// </summary>
            public void ReleaseWorker(int recordsProcessed)
            {
                if (Interlocked.Increment(ref this.m_availableWorkers) > 0)
                {
                    this.m_loadEvent.Set(); // Allow loading of records
                    this.m_processEvent.Reset();
                }
                Interlocked.Add(ref this.m_recordsProcessed, recordsProcessed);

            }

            /// <summary>
            /// Dispose of this context
            /// </summary>
            public void Dispose()
            {
                this.m_isRunning = false;
                while (this.m_availableWorkers != this.m_maxWorkers || // still someone processing
                        !this.m_entityStack.IsEmpty && this.m_haltException == null)
                {
                    this.m_processEvent.Set();
                    Thread.Sleep(1000);
                }
                this.m_loadEvent.Dispose();
                this.m_processEvent.Dispose();
            }
        }

        // Data manager
        private MdmDataManager<TEntity> m_dataManager;

        // Batch persistence
        private IDataPersistenceService<Bundle> m_batchPersistence;

        // Entity persistence service
        private IDataPersistenceService<TEntity> m_entityPersistence;

        // Relationship persistence
        private IDataPersistenceServiceEx<EntityRelationship> m_relationshipPersistence;

        // Relationship persistence
        private readonly IThreadPoolService m_threadPool;

        // Pep service
        private IPolicyEnforcementService m_pepService;

        /// <summary>
        /// Fired when progress of this object changes
        /// </summary>
        public event EventHandler<ProgressChangedEventArgs> ProgressChanged;

        // Disposed
        private bool m_disposed = false;

        /// <summary>
        /// Creates a new entity merger service
        /// </summary>
        public MdmEntityMerger(IDataPersistenceService<Bundle> batchService, IThreadPoolService threadPool, IPolicyEnforcementService policyEnforcement, IDataPersistenceService<TEntity> persistenceService, IDataPersistenceServiceEx<EntityRelationship> relationshipService)
        {
            this.m_dataManager = MdmDataManagerFactory.GetDataManager<TEntity>();
            this.m_batchPersistence = batchService;
            this.m_pepService = policyEnforcement;
            this.m_entityPersistence = persistenceService;
            this.m_relationshipPersistence = relationshipService;
            this.m_threadPool = threadPool;
            if (this.m_relationshipPersistence is IReportProgressChanged irpc)
            {
                irpc.ProgressChanged += (o, e) => this.ProgressChanged?.Invoke(o, e); // pass through progress reports
            }
        }

        /// <summary>
        /// Get the ignore list
        /// </summary>
        public override IEnumerable<Guid> GetIgnoredKeys(Guid masterKey) => this.m_dataManager.GetIgnoredMasters(masterKey).Select(o => o.Key.Value);

        /// <summary>
        /// Gets ignored records
        /// </summary>
        public override IQueryResultSet<IdentifiedData> GetIgnored(Guid masterKey)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);

            if (this.m_dataManager.IsMaster(masterKey))
            {
                return new TransformQueryResultSet<ITargetedAssociation, IdentifiedData>(this.m_dataManager.GetIgnoredCandidateLocals(masterKey), o => ((EntityRelationship)o).LoadProperty(p => p.SourceEntity));
            }
            else
            {
                return new TransformQueryResultSet<ITargetedAssociation, IdentifiedData>(this.m_dataManager.GetIgnoredMasters(masterKey),
                    o => ((EntityRelationship)o).LoadProperty(p => p.TargetEntity));
            }
        }

        /// <summary>
        /// Get candidate associations
        /// </summary>
        public override IEnumerable<Guid> GetMergeCandidateKeys(Guid masterKey) => this.m_dataManager.GetCandidateLocals(masterKey).Select(o => o.Key.Value);

        /// <summary>
        /// Get merge candidates
        /// </summary>
        public override IQueryResultSet<IdentifiedData> GetMergeCandidates(Guid masterKey)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);

            if (this.m_dataManager.IsMaster(masterKey))
            {
                return new TransformQueryResultSet<ITargetedAssociation, IdentifiedData>(this.m_dataManager.GetCandidateLocals(masterKey),
                    o =>
                    {
                        var retVal = o.LoadProperty(p => p.SourceEntity) as Entity;
                        if (o is EntityRelationship e)
                        {
                            retVal.AddTag(SystemTagNames.MatchScoreTag, $"{e.Strength:0#%}");
                        }
                        return retVal;
                    });
            }
            else
            {
                return new TransformQueryResultSet<ITargetedAssociation, IdentifiedData>(this.m_dataManager.GetEstablishedCandidateMasters(masterKey), o =>
                    {
                        var retVal = o.LoadProperty(p => p.TargetEntity) as Entity;
                        if (o is EntityRelationship e)
                        {
                            retVal.AddTag(SystemTagNames.MatchScoreTag, $"{e.Strength:#0%}");
                        }
                        return retVal;
                    });
            }
        }

        /// <summary>
        /// Adds an ignore clause to either the master or the targets
        /// </summary>
        public override IdentifiedData Ignore(Guid masterKey, IEnumerable<Guid> falsePositives)
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);

                Bundle transaction = new Bundle();
                transaction.AddRange(falsePositives.SelectMany(o => this.m_dataManager.MdmTxIgnoreCandidateMatch(masterKey, o, transaction.Item)));
                // Commit the transaction
                return this.m_batchPersistence.Insert(transaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);
            }
            catch (Exception ex)
            {
                throw new MdmException("Error performing ignore", ex);
            }
        }

        /// <summary>
        /// Merge the specified records together
        /// </summary>
        public override RecordMergeResult Merge(Guid survivorKey, IEnumerable<Guid> linkedDuplicates)
        {
            try
            {
                // Merging
                if (this.FireMerging(survivorKey, linkedDuplicates))
                {
                    this.m_tracer.TraceWarning("Pre-Event Handler for merge indicated cancel on {0}", survivorKey);
                    return new RecordMergeResult(RecordMergeStatus.Aborted, null, null);
                }

                // We want to get the target
                RecordMergeStatus recordMergeStatus = RecordMergeStatus.Success;
                var survivor = this.m_dataManager.GetRaw(survivorKey) as Entity;
                bool isSurvivorMaster = this.m_dataManager.IsMaster(survivorKey);
                if (isSurvivorMaster)
                {
                    try
                    {
                        // Trying to write to master - do they have permission?
                        this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
                    }
                    catch (PolicyViolationException e) when (e.PolicyId == MdmPermissionPolicyIdentifiers.WriteMdmMaster)
                    {
                        survivor = (TEntity)this.m_dataManager.GetLocalFor(survivorKey, AuthenticationContext.Current.Principal);
                        if (survivor == null)
                        {
                            throw new DetectedIssueException(Core.BusinessRules.DetectedIssuePriorityType.Error, MdmConstants.INVALID_MERGE_ISSUE, $"Principal has no authority to merge into {survivorKey}", DetectedIssueKeys.SecurityIssue, e);
                        }

                        recordMergeStatus = RecordMergeStatus.Alternate;
                        isSurvivorMaster = false;
                    }
                }

                Bundle transactionBundle = new Bundle();

                // For each linked duplicate
                var replaced = linkedDuplicates.Select(itm =>
                {
                    var victim = this.m_dataManager.GetRaw(itm) as Entity;
                    var isVictimMaster = this.m_dataManager.IsMaster(itm);
                    if (isVictimMaster)
                    {
                        try
                        {
                            // Trying to write to master - do they have permission?
                            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.MergeMdmMaster);
                        }
                        catch (PolicyViolationException e) when (e.PolicyId == MdmPermissionPolicyIdentifiers.MergeMdmMaster)
                        {
                            victim = (TEntity)this.m_dataManager.GetLocalFor(itm, AuthenticationContext.Current.Principal);
                            if (victim == null)
                            {
                                throw new DetectedIssueException(Core.BusinessRules.DetectedIssuePriorityType.Error, MdmConstants.INVALID_MERGE_ISSUE, $"Principal has no authority to merge {itm}", DetectedIssueKeys.SecurityIssue, e);
                            }
                            isVictimMaster = false;
                            recordMergeStatus = RecordMergeStatus.Alternate;
                        }
                    }

                    // Sanity check
                    if (victim.Key == survivor.Key)
                    {
                        throw new DetectedIssueException(DetectedIssuePriorityType.Error, MdmConstants.INVALID_MERGE_ISSUE, "Records cannot be merged into themselves", DetectedIssueKeys.FormalConstraintIssue, null);
                    }

                    // MASTER>MASTER
                    if (isSurvivorMaster && isVictimMaster) // MASTER>MASTER
                    {
                        this.m_tracer.TraceInfo("MASTER({0})>MASTER({0}) MERGE", victim.Key, survivor.Key);
                        transactionBundle.AddRange(this.m_dataManager.MdmTxMergeMasters(survivorKey, itm, transactionBundle.Item));
                        recordMergeStatus |= RecordMergeStatus.DestructiveMerge;
                    }
                    else if (isSurvivorMaster && !isVictimMaster) // LOCAL>MASTER = LINK
                    {
                        // Ensure that the local manipulation is allowed
                        if (!this.m_dataManager.IsOwner((TEntity)victim, AuthenticationContext.Current.Principal))
                        {
                            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm); // MUST BE ABLE TO MANIPULATE OTHER LOCALS
                        }
                        this.m_tracer.TraceInfo("LOCAL({0})>MASTER({0}) MERGE", victim.Key, survivor.Key);
                        transactionBundle.AddRange(this.m_dataManager.MdmTxMasterLink(survivorKey, victim.Key.Value, transactionBundle.Item, true));
                        recordMergeStatus |= RecordMergeStatus.LinkInsteadOfMerge;

                    }
                    else if (!isSurvivorMaster && !isVictimMaster) // LOCAL>LOCAL = MERGE
                    {

                        if (!this.m_dataManager.IsOwner(victim.Key.Value, AuthenticationContext.Current.Principal) ||
                            !this.m_dataManager.IsOwner(survivor.Key.Value, AuthenticationContext.Current.Principal)
                            )
                        {
                            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm); // MUST BE ABLE TO MANIPULATE OTHER LOCALS
                        }
                        // First, target replaces victim
                        transactionBundle.Add(new EntityRelationship(EntityRelationshipTypeKeys.Replaces, survivor.Key, victim.Key, null)
                        {
                            RelationshipRoleKey = EntityRelationshipTypeKeys.Duplicate
                        });
                        this.m_tracer.TraceInfo("LOCAL({0})>LOCAL({0}) MERGE", victim.Key, survivor.Key);

                        // Obsolete the victim - the victim is obsolete since it was accurate and is no longer the accurate
                        victim.StatusConceptKey = StatusKeys.Inactive;
                        transactionBundle.Add(victim);

                        // Obsolete the old identifiers over
                        transactionBundle.AddRange(
                            victim.LoadCollection(o => o.Identifiers).Where(i => !survivor.LoadCollection(o => o.Identifiers).Any(e => e.SemanticEquals(i))).Select(o =>
                            {
                                o.BatchOperation = BatchOperationType.Delete;
                                return o;
                            })
                        );

                        // Copy identifiers over
                        transactionBundle.AddRange(
                            victim.LoadCollection(o => o.Identifiers).Where(i => !survivor.LoadCollection(o => o.Identifiers).Any(e => e.SemanticEquals(i))).Select(o => new EntityIdentifier(o.IdentityDomain, o.Value)
                            {
                                SourceEntityKey = survivor.Key,
                                IssueDate = o.IssueDate,
                                ExpiryDate = o.ExpiryDate
                            })
                        );

                        // Remove links from victim
                        foreach (var rel in this.m_dataManager.GetAllMdmAssociations(victim.Key.Value).OfType<EntityRelationship>())
                        {
                            rel.BatchOperation = BatchOperationType.Delete;
                            transactionBundle.Add(rel);
                        }

                        // Recheck the master to ensure that it isn't dangling out here
                        var otherLocals = this.m_relationshipPersistence.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.TargetEntityKey == itm && o.SourceEntityKey != victim.Key, AuthenticationContext.SystemPrincipal).Any();
                        if (!otherLocals)
                        {
                            transactionBundle.Add(new Entity()
                            {
                                BatchOperation = BatchOperationType.Delete,
                                Key = itm
                            });
                        }
                        recordMergeStatus |= RecordMergeStatus.DestructiveMerge;

                    }
                    else
                    {
                        throw new MdmException($"Cannot determine viable merge/link strategy between {survivor.Key} and {victim.Key}", null);
                    }

                    return victim.Key.Value;
                }).ToArray();

                var inserted = this.m_batchPersistence.Insert(transactionBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);

                // Trigger appropriate events
                if (recordMergeStatus.HasFlag(RecordMergeStatus.DestructiveMerge))
                {
                    this.FireMerged(survivor.Key.Value, replaced);
                }
                else if (recordMergeStatus.HasFlag(RecordMergeStatus.LinkInsteadOfMerge))
                {
                    inserted.Item.ForEach(o =>
                    {
                        if (o is ITargetedAssociation ita && ita.AssociationTypeKey == MdmConstants.MasterRecordRelationship)
                        {
                            switch (o.BatchOperation)
                            {
                                case BatchOperationType.Insert:
                                    m_dataManager.FireManagedLinkEstablished(ita);
                                    break;
                                case BatchOperationType.Delete:
                                    m_dataManager.FireManagedLinkRemoved(ita);
                                    break;
                            }
                        }
                    });
                }
                return new RecordMergeResult(recordMergeStatus, new Guid[] { survivor.Key.Value }, replaced);
            }
            catch (Exception ex)
            {
                this.m_tracer.TraceError("Error performing MDM merging operation on {0}: {1}", survivorKey, ex);
                throw new MdmException($"Error performing MDM merge on {survivorKey}", ex);
            }
        }

        /// <summary>
        /// TODO: Remove the MDM Ignore Relationships
        /// </summary>
        public override IdentifiedData UnIgnore(Guid masterKey, IEnumerable<Guid> ignoredKeys)
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);

                Bundle transaction = new Bundle();
                transaction.AddRange(ignoredKeys.SelectMany(o => this.m_dataManager.MdmTxUnIgnoreCandidateMatch(masterKey, o, transaction.Item)));
                // Commit the transaction
                return this.m_batchPersistence.Insert(transaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);
            }
            catch (Exception ex)
            {
                throw new MdmException("Error performing ignore", ex);
            }
        }

        /// <summary>
        /// TODO: Separate locals from their master
        /// </summary>
        public override RecordMergeResult Unmerge(Guid masterKey, Guid unmergeDuplicateKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get merge candidates
        /// </summary>
        public override IQueryResultSet<ITargetedAssociation> GetGlobalMergeCandidates()
        {
            return this.m_dataManager.GetAllMdmCandidateLocals();
        }

        /// <summary>
        /// Detect global merge candidates
        /// </summary>
        public override void DetectGlobalMergeCandidates()
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

                // Fetch all locals
                int maxWorkers = Environment.ProcessorCount / 3;
                if (maxWorkers == 0)
                {
                    maxWorkers = 1;
                }

                using (var matchContext = new BackgroundMatchContext(maxWorkers))
                {

                    // Matcher queue
                    this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(0f, $"Gathering sources..."));

                    for (var i = 0; i < maxWorkers; i++)
                    {
                        this.m_threadPool.QueueUserWorkItem(this.BackgroundMatchProcess, matchContext);
                    }

                    // Main matching loop - 
                    try
                    {
                        var recordsToProcess = this.m_entityPersistence.Query(o => StatusKeys.ActiveStates.Contains(o.StatusConceptKey.Value) && o.DeterminerConceptKey != MdmConstants.RecordOfTruthDeterminer, AuthenticationContext.SystemPrincipal);
                        var totalRecords = recordsToProcess.Count();
                        var rps = 0.0f;
                        var sw = new Stopwatch();
                        var nRecordsLoaded = 0;
                        sw.Start();

                        using (DataPersistenceControlContext.Create(LoadMode.QuickLoad))
                        {
                            foreach (var itm in recordsToProcess)
                            {
                                this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(nRecordsLoaded++ / (float)totalRecords, $"Matching {matchContext.RecordsProcessed} recs @ {rps:#.#} r/s"));
                                rps = 1000.0f * (float)matchContext.RecordsProcessed / (float)sw.ElapsedMilliseconds;
                                matchContext.QueueLoadedRecord(itm);
                            }
                        }

                        sw.Stop();
                    }
                    catch (Exception e)
                    {
                        matchContext.Halt(e);
                    }
                    this.m_tracer.TraceVerbose("DetectGlobalMergeCandidate: Completed matching");
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error running detect of global merge candidates", e);
            }
        }

        /// <summary>
        /// Process background matches
        /// </summary>
        private void BackgroundMatchProcess(BackgroundMatchContext matchContext)
        {
            try
            {
                while (matchContext.IsRunning)
                {
                    var records = new TEntity[16];
                    var nRecords = 0;
                    while ((nRecords = matchContext.DeQueueLoadedRecords(records)) > 0)
                    {
                        try
                        {
                            var matches = records.Take(nRecords).SelectMany(o => this.m_dataManager.MdmTxMatchMasters(o, new IdentifiedData[0]));
                            this.m_batchPersistence.Insert(new Bundle(matches), TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                        }
                        finally
                        {
                            matchContext.ReleaseWorker(nRecords);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                matchContext.Halt(e);
            }
        }

        /// <summary>
        /// Clear global merge candidates
        /// </summary>
        public override void ClearGlobalMergeCanadidates()
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

                this.m_tracer.TraceInfo("Clearing MDM merge candidates...");

                // TODO: When the persistence refactor is done - change this to use the bulk method
                var classKeys = typeof(TEntity).GetCustomAttributes<ClassConceptKeyAttribute>(false).Select(o => Guid.Parse(o.ClassConcept));

                using (DataPersistenceControlContext.Create(DeleteMode.PermanentDelete).WithName("Clearing Candidates"))
                {
                    this.m_relationshipPersistence.DeleteAll(o => classKeys.Contains(o.SourceEntity.ClassConceptKey.Value) && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ClassificationKey == MdmConstants.AutomagicClassification, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
            }
            catch (Exception e)
            {
                this.m_tracer.TraceError("Error clearing MDM merge candidates: {0}", e);
                throw new MdmException("Error clearing MDM merge candidates", e);
            }
        }

        /// <summary>
        /// Clear global merge candidates
        /// </summary>
        public override void ClearGlobalIgnoreFlags()
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

                this.m_tracer.TraceInfo("Clearing MDM ignore candidates...");

                // TODO: When the persistence refactor is done - change this to use the bulk method
                var classKeys = typeof(TEntity).GetCustomAttributes<ClassConceptKeyAttribute>(false).Select(o => Guid.Parse(o.ClassConcept));
                using (DataPersistenceControlContext.Create(DeleteMode.PermanentDelete).WithName("Clearing Ignore Flags"))
                {
                    this.m_relationshipPersistence.DeleteAll(o => classKeys.Contains(o.SourceEntity.ClassConceptKey.Value) && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, TransactionMode.Commit, AuthenticationContext.Current.Principal);

                }
            }
            catch (Exception e)
            {
                this.m_tracer.TraceError("Error clearing MDM ignore links: {0}", e);
                throw new MdmException("Error clearing MDM ignore links", e);
            }
        }

        /// <summary>
        /// Reset all links and all MDM data in this database
        /// </summary>
        public override void Reset(bool includeVerified, bool linksOnly)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

            throw new NotImplementedException("This function is not yet implemented");
        }

        /// <summary>
        /// Reset the specified object of all MDM data including resetting the master links
        /// </summary>
        public override void Reset(Guid masterKey, bool includeVerified, bool linksOnly)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

            throw new NotImplementedException("This function is not yet implemented");
        }

        /// <summary>
        /// Clear merge candidates for the specified key
        /// </summary>
        public override void ClearMergeCandidates(Guid masterKey)
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

                this.m_tracer.TraceInfo("Clearing MDM merge candidates for {0}...", masterKey);

                // Determine if the parameter is a master or a local
                Expression<Func<EntityRelationship, bool>> expr = null;
                if (this.m_dataManager.IsMaster(masterKey))
                {
                    expr = o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null;
                }
                else
                {
                    expr = o => o.SourceEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null;
                }

                // TODO: When the persistence refactor is done - change this to use the bulk method
                using (DataPersistenceControlContext.Create(DeleteMode.PermanentDelete))
                {
                    this.m_relationshipPersistence.DeleteAll(expr, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
            }
            catch (Exception e)
            {
                this.m_tracer.TraceError("Error clearing MDM merge candidates for {0}: {1}", masterKey, e);
                throw new MdmException($"Error clearing MDM merge candidates for {masterKey}", e);
            }
        }

        /// <summary>
        /// Clear all ignore flags on the specified master
        /// </summary>
        public override void ClearIgnoreFlags(Guid masterKey)
        {
            try
            {
                this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);
                this.m_tracer.TraceInfo("Clearing MDM merignore flags for {0}...", masterKey);

                // Determine if the parameter is a master or a local
                Expression<Func<EntityRelationship, bool>> expr = null;
                if (this.m_dataManager.IsMaster(masterKey))
                {
                    expr = o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null;
                }
                else
                {
                    expr = o => o.SourceEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null;
                }

                using (DataPersistenceControlContext.Create(DeleteMode.PermanentDelete))
                {
                    this.m_relationshipPersistence.DeleteAll(expr, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
            }
            catch (Exception e)
            {
                this.m_tracer.TraceError("Error clearing MDM merge candidates for {0}: {1}", masterKey, e);
                throw new MdmException($"Error clearing MDM merge candidates for {masterKey}", e);
            }
        }

        /// <summary>
        /// Dispose of this object (shuts down any threads)
        /// </summary>
        public void Dispose()
        {
            this.m_disposed = true;
        }
    }
}