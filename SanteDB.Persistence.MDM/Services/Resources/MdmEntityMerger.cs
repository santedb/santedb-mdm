using SanteDB.Core.Event;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// An MDM merger that operates on Entities
    /// </summary>
    /// <remarks>This class exists to allow callers to interact with the operations in the underlying infrastructure.</remarks>
    public class MdmEntityMerger<TEntity> : MdmResourceMerger<TEntity>
        where TEntity : Entity, new()
    {

        // Data manager
        private MdmDataManager<TEntity> m_dataManager;

        // Batch persistence
        private IDataPersistenceService<Bundle> m_batchPersistence;

        // Pep service
        private IPolicyEnforcementService m_pepService;

        /// <summary>
        /// Creates a new entity merger service 
        /// </summary>
        public MdmEntityMerger(IDataPersistenceService<Bundle> batchService, IPolicyEnforcementService policyEnforcement)
        {
            this.m_dataManager = MdmDataManagerFactory.GetDataManager<TEntity>();
            this.m_batchPersistence = batchService;
            this.m_pepService = policyEnforcement;
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
            if(masterKey == Guid.Empty)
            {
                return this.m_dataManager.GetAllMdmCandidateLocals();
            }
            else if (this.m_dataManager.IsMaster(masterKey))
            {
                return this.m_dataManager.GetCandidateLocals(masterKey)
                    .OfType<EntityRelationship>()
                    .Select(o => {
                       var retVal = o.LoadProperty(p => p.SourceEntity) as Entity;
                        retVal.AddTag("$match.score", $"{o.Strength:%%.%%}");
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
                        retVal.AddTag("$match.score", $"{o.Strength:%%.%%}");
                        return retVal;
                    });
            }
        }

        /// <summary>
        /// Adds an ignore clause to either the master or the targets
        /// </summary>
        public override void Ignore(Guid masterKey, IEnumerable<Guid> falsePositives)
        {
            try
            {

                Bundle transaction = new Bundle();
                transaction.AddRange(falsePositives.SelectMany(o => this.m_dataManager.MdmTxIgnoreCandidateMatch(masterKey, o, transaction.Item)));
                // Commit the transaction
                this.m_batchPersistence.Insert(transaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);
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
                            throw new MdmException(survivor, $"Cannot find writeable LOCAL for {survivorKey}", e);
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
                                throw new MdmException(survivor, $"Cannot find writeable LOCAL for {itm}", e);
                            }
                            isVictimMaster = false;
                            recordMergeStatus = RecordMergeStatus.Alternate;
                        }
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

                        // Obsolete the victim
                        victim.StatusConceptKey = StatusKeys.Obsolete;
                        transactionBundle.Add(victim);

                        // Copy identifiers over
                        transactionBundle.AddRange(
                            victim.LoadCollection(o => o.Identifiers).Where(i => !survivor.LoadCollection(o => o.Identifiers).Any(e => e.SemanticEquals(i))).Select(o => new EntityIdentifier(o.Authority, o.Value)
                            {
                                EffectiveVersionSequenceId = o.EffectiveVersionSequenceId,
                                SourceEntityKey = survivor.Key,
                                IssueDate = o.IssueDate,
                                ExpiryDate = o.ExpiryDate
                            })
                        );

                        // Remove links from victim
                        foreach (var rel in this.m_dataManager.GetAllMdmAssociations(victim.Key.Value).OfType<EntityRelationship>())
                        {
                            rel.ObsoleteVersionSequenceId = Int32.MaxValue;
                            transactionBundle.Add(rel);
                        }

                    }
                    else
                    {
                        throw new MdmException($"Cannot determine viable merge/link strategy between {survivor.Key} and {victim.Key}", null);
                    }

                    return victim.Key.Value;

                }).ToArray();

                this.m_batchPersistence.Insert(transactionBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                this.FireMerged(survivorKey, linkedDuplicates);
                return new RecordMergeResult(recordMergeStatus, new Guid[] { survivor.Key.Value }, replaced);
            }
            catch (Exception ex)
            {
                this.m_tracer.TraceError("Error performing MDM merging operation on {0}: {1}", survivorKey, ex);
                throw new MdmException($"Error performing MDM merge on {survivorKey}", ex);
            }
        }

        public override void UnIgnore(Guid masterKey, IEnumerable<Guid> ignoredKeys)
        {
            throw new NotImplementedException();
        }

        public override RecordMergeResult Unmerge(Guid masterKey, Guid unmergeDuplicateKey)
        {
            throw new NotImplementedException();
        }
    }
}
