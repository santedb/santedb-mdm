/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-5
 */

using System.Reflection;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Event;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Attributes;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Security;
using System.Text;
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


        // Data manager
        private MdmDataManager<TEntity> m_dataManager;

        // Batch persistence
        private IDataPersistenceService<Bundle> m_batchPersistence;

        // Entity persistence service
        private IStoredQueryDataPersistenceService<TEntity> m_entityPersistence;

        // Relationship persistence
        private IStoredQueryDataPersistenceService<EntityRelationship> m_relationshipPersistence;
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
        public MdmEntityMerger(IDataPersistenceService<Bundle> batchService, IThreadPoolService threadPool, IPolicyEnforcementService policyEnforcement, IStoredQueryDataPersistenceService<TEntity> persistenceService, IStoredQueryDataPersistenceService<EntityRelationship> relationshipPersistence)
        {
            this.m_dataManager = MdmDataManagerFactory.GetDataManager<TEntity>();
            this.m_batchPersistence = batchService;
            this.m_pepService = policyEnforcement;
            this.m_entityPersistence = persistenceService;
            this.m_relationshipPersistence = relationshipPersistence;
            this.m_threadPool = threadPool;
            if(this.m_relationshipPersistence is IReportProgressChanged irpc)
            {
                irpc.ProgressChanged += (o, e) => this.ProgressChanged?.Invoke(o, e); // pass through progress reports
            }
        }

        /// <summary>
        /// Get the ignore list
        /// </summary>
        public override IEnumerable<Guid> GetIgnoredKeys(Guid masterKey) => this.GetIgnored(masterKey).Select(o => o.Key.Value);

        /// <summary>
        /// Gets ignored records
        /// </summary>
        public override IEnumerable<IdentifiedData> GetIgnored(Guid masterKey)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);

            if (this.m_dataManager.IsMaster(masterKey))
            {
                return this.m_dataManager.GetIgnoredCandidateLocals(masterKey)
                    .Select(o => o.LoadProperty(p => p.SourceEntity) as IdentifiedData);
            }
            else
            {
                return this.m_dataManager.GetIgnoredMasters(masterKey)
                    .Select(o => o.LoadProperty(p => p.TargetEntity) as IdentifiedData);
            }
        }

        /// <summary>
        /// Get candidate associations
        /// </summary>
        public override IEnumerable<Guid> GetMergeCandidateKeys(Guid masterKey) => this.GetMergeCandidates(masterKey).Select(o => o.Key.Value);

        /// <summary>
        /// Get merge candidates
        /// </summary>
        public override IEnumerable<IdentifiedData> GetMergeCandidates(Guid masterKey)
        {
            this.m_pepService.Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);

            if (this.m_dataManager.IsMaster(masterKey))
            {
                return this.m_dataManager.GetCandidateLocals(masterKey)
                    .OfType<EntityRelationship>()
                    .Select(o =>
                    {
                        var retVal = o.LoadProperty(p => p.SourceEntity) as Entity;
                        retVal.AddTag("$match.score", $"{o.Strength:0#%}");
                        return retVal;
                    });
            }
            else
            {
                return this.m_dataManager.GetEstablishedCandidateMasters(masterKey)
                    .OfType<EntityRelationship>()
                    .Select(o =>
                    {
                        var retVal = o.LoadProperty(p => p.TargetEntity) as Entity;
                        retVal.AddTag("$match.score", $"{o.Strength:#0%}");
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
                    return new RecordMergeResult(RecordMergeStatus.Cancelled, null, null);
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
                        survivor = this.m_dataManager.GetLocalFor(survivorKey, AuthenticationContext.Current.Principal);
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
                            victim = this.m_dataManager.GetLocalFor(itm, AuthenticationContext.Current.Principal);
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
                    }
                    else if (!isSurvivorMaster && !isVictimMaster) // LOCAL>LOCAL = MERGE
                    {
                        // First, target replaces victim
                        transactionBundle.Add(new EntityRelationship(EntityRelationshipTypeKeys.Replaces, survivor.Key, victim.Key, null)
                        {
                            RelationshipRoleKey = EntityRelationshipTypeKeys.Duplicate
                        });
                        this.m_tracer.TraceInfo("LOCAL({0})>LOCAL({0}) MERGE", victim.Key, survivor.Key);

                        // Obsolete the victim - the victim is obsolete since it was accurate and is no longer the accurate
                        victim.StatusConceptKey = StatusKeys.Obsolete;
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
                            victim.LoadCollection(o => o.Identifiers).Where(i => !survivor.LoadCollection(o => o.Identifiers).Any(e => e.SemanticEquals(i))).Select(o => new EntityIdentifier(o.Authority, o.Value)
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
                        var otherLocals = this.m_relationshipPersistence.Count(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.TargetEntityKey == itm && o.SourceEntityKey != victim.Key, AuthenticationContext.SystemPrincipal);
                        if (otherLocals == 0)
                        {
                            transactionBundle.Add(new Entity()
                            {
                                BatchOperation = BatchOperationType.Delete,
                                Key = itm
                            });
                        }
                    }
                    else
                    {
                        throw new MdmException($"Cannot determine viable merge/link strategy between {survivor.Key} and {victim.Key}", null);
                    }

                    return victim.Key.Value;
                }).ToArray();

                this.m_batchPersistence.Insert(transactionBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                this.FireMerged(survivor.Key.Value, replaced);
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
        public override IEnumerable<ITargetedAssociation> GetGlobalMergeCandidates(int offset, int count, out int totalCount)
        {
            return this.m_dataManager.GetAllMdmCandidateLocals(offset, count, out totalCount);
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
                // TODO: Update to the new persistence layer
                Guid queryId = Guid.NewGuid();
                int offset = 0, totalResults = 1, batchSize = 1000;

                var processStack = new ConcurrentStack<TEntity>();
                var qps = this.m_entityPersistence as IFastQueryDataPersistenceService<TEntity>;
                var fetchQueue = new ConcurrentQueue<TEntity>();
                var writeQueue = new ConcurrentQueue<Bundle>();
                var fetchEvent = new ManualResetEventSlim(false);
                var writeEvent = new ManualResetEventSlim(false);
                long completeProcess = 0;
                bool completeProcessing = false;

                // Matcher queue
                this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(0f, $"Gathering sources..."));

                this.m_threadPool.QueueUserWorkItem(_ =>
                {
                    while (!completeProcessing&& !this.m_disposed)
                    {
                        var dr = Interlocked.Read(ref completeProcess);
                        this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)dr / (float)totalResults, $"Matching {dr:#,###,###} of {totalResults:#,###,###} (Writer: {writeQueue.Count})"));
                        Thread.Sleep(1000);
                    }
                });
                this.m_threadPool.QueueUserWorkItem(_ =>
                {
                    var processList = new TEntity[Environment.ProcessorCount * 2];
                    int idx = 0;
                    while ((!completeProcessing || !fetchQueue.IsEmpty) && !this.m_disposed)
                    {
                        fetchEvent.Wait();
                        if (writeQueue.IsEmpty || fetchQueue.Count > 50)
                        { // wait for write queue to empty  
                            while (fetchQueue.TryDequeue(out var candidate))
                            {
                                processList[idx++] = candidate;
                                if (idx == processList.Length)
                                {
                                    this.m_threadPool.QueueUserWorkItem<TEntity[]>(o =>
                                    {
                                        writeQueue.Enqueue(new Bundle(o.SelectMany(r => this.m_dataManager.MdmTxMatchMasters(r, new IdentifiedData[0]))));
                                        writeEvent.Set();
                                        Interlocked.Add(ref completeProcess, o.Length);
                                    }, processList.ToArray());
                                    idx = 0;
                                }
                            }
                        }
                        fetchEvent.Reset();
                    }

                    writeQueue.Enqueue(new Bundle(processList.Take(idx).SelectMany(r => this.m_dataManager.MdmTxMatchMasters(r, new IdentifiedData[0]))));
                    writeEvent.Set();
                });

                this.m_threadPool.QueueUserWorkItem(_ =>
                {
                    while ((!completeProcessing || !writeQueue.IsEmpty) && !this.m_disposed)
                    {
                        writeEvent.Wait();

                        var batchBundle = new Bundle();
                        while (writeQueue.TryDequeue(out var bundle))
                        {
                            if (bundle.Item.Count > 0)
                            {
                                this.m_batchPersistence.Insert(bundle, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                            }
                        }
                        fetchEvent.Set(); // Notify the queue

                        writeEvent.Reset();
                    }
                });

                while (offset < totalResults)
                {
                    foreach (var itm in qps.QueryFast(o => StatusKeys.ActiveStates.Contains(o.StatusConceptKey.Value) && o.DeterminerConceptKey != MdmConstants.RecordOfTruthDeterminer, queryId, offset, batchSize, out totalResults, AuthenticationContext.SystemPrincipal))
                    {
                        fetchQueue.Enqueue(itm);
                    }
                    fetchEvent.Set();

                    offset += batchSize;
                }

                while (!writeQueue.IsEmpty || !fetchQueue.IsEmpty)
                {
                    this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)completeProcess / (float)totalResults, $"Finalizing ({writeQueue.Count + fetchQueue.Count:#,###,###} remain)"));
                    Thread.Sleep(1000);
                }
                completeProcessing = true; // let threads die
            }
            catch (Exception e)
            {
                throw new Exception("Error running detect of global merge candidiates", e);
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
                Expression<Func<EntityRelationship, bool>> purgeExpression = o => classKeys.Contains( o.SourceEntity.ClassConceptKey.Value) && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null;
                if (this.m_relationshipPersistence is IBulkDataPersistenceService ibds)
                {
                    ibds.Purge(TransactionMode.Commit, AuthenticationContext.SystemPrincipal, purgeExpression);
                }
                else
                {
                    Guid queryId = Guid.NewGuid();
                    int offset = 0, totalResults = 1, batchSize = 500;
                    while (offset < totalResults)
                    {
                        this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)offset / (float)totalResults, "Clearing Candidates"));

                        var results = this.m_relationshipPersistence.Query(purgeExpression, queryId, offset, batchSize, out totalResults, AuthenticationContext.SystemPrincipal); ;
                        var batch = new Bundle(results.Select(o => { o.BatchOperation = BatchOperationType.Delete; return o; }));
                        this.m_batchPersistence.Update(batch, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                        offset += batchSize;
                    }
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

                this.m_tracer.TraceInfo("Clearing MDM ignore flags...");

                // TODO: When the persistence refactor is done - change this to use the bulk method
                Guid queryId = Guid.NewGuid();
                int offset = 0, totalResults = 1, batchSize = 50;
                while (offset < totalResults)
                {
                    var results = this.m_relationshipPersistence.Query(o => o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, queryId, offset, batchSize, out totalResults, AuthenticationContext.SystemPrincipal); ;
                    var batch = new Bundle(results.Select(o => { o.BatchOperation = BatchOperationType.Delete; return o; }));
                    this.m_batchPersistence.Update(batch, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                    offset += batchSize;
                    this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)offset / (float)totalResults, "Clearing ignore links"));
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

            throw new NotImplementedException("This function is not yet impleented");
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
                Guid queryId = Guid.NewGuid();
                int offset = 0, totalResults = 1, batchSize = 100;
                while (offset < totalResults)
                {
                    var results = this.m_relationshipPersistence.Query(expr, queryId, offset, batchSize, out totalResults, AuthenticationContext.SystemPrincipal); ;
                    var batch = new Bundle(results.Select(o => { o.BatchOperation = BatchOperationType.Delete; return o; }));
                    this.m_batchPersistence.Update(batch, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                    offset += batchSize;
                    this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)offset / (float)totalResults, "Clearing candidate links"));
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

                // TODO: When the persistence refactor is done - change this to use the bulk method
                Guid queryId = Guid.NewGuid();
                int offset = 0, totalResults = 1, batchSize = 100;
                while (offset < totalResults)
                {
                    var results = this.m_relationshipPersistence.Query(expr, queryId, offset, batchSize, out totalResults, AuthenticationContext.SystemPrincipal); ;
                    var batch = new Bundle(results.Select(o => { o.BatchOperation = BatchOperationType.Delete; return o; }));
                    this.m_batchPersistence.Update(batch, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                    offset += batchSize;
                    this.ProgressChanged?.Invoke(this, new ProgressChangedEventArgs((float)offset / (float)totalResults, "Clearing ignore links"));
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