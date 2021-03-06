﻿using SanteDB.Core;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Model.Serialization;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Security.Principal;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Data manager for MDM services for entities
    /// </summary>
    public class MdmEntityDataManager<TModel> : MdmDataManager<TModel>
        where TModel : Entity, new()
    {

        // Tracer
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // Entity persistence serviuce
        private IDataPersistenceService<Entity> m_entityPersistenceService;

        // Persistence service
        private IDataPersistenceService<TModel> m_persistenceService;

        // Relationship service
        private IDataPersistenceService<EntityRelationship> m_relationshipService;

        // Matching service
        private IRecordMatchingService m_matchingService;

        // Configuration
        private ResourceMergeConfiguration m_resourceConfiguration;

        /// <summary>
        /// Create entity data manager
        /// </summary>
        public MdmEntityDataManager(ResourceMergeConfiguration configuration) : base(ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>() as IDataPersistenceService)
        {
            ModelSerializationBinder.RegisterModelType(typeof(EntityMaster<TModel>));
            ModelSerializationBinder.RegisterModelType($"{typeof(TModel).Name}Master", typeof(EntityMaster<TModel>));

            this.m_resourceConfiguration = configuration;
            this.m_entityPersistenceService = base.m_underlyingTypePersistence as IDataPersistenceService<Entity>;
            this.m_persistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<TModel>>();
            this.m_relationshipService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>();
            this.m_matchingService = ApplicationServiceContext.Current.GetService<IRecordMatchingService>();
        }


        /// <summary>
        /// Determine is the specified data is a master key
        /// </summary>
        public override bool IsMaster(Guid dataKey)
        {
            return this.m_entityPersistenceService.Get(dataKey, null, true, AuthenticationContext.SystemPrincipal).ClassConceptKey == MdmConstants.MasterRecordClassification;
        }

        /// <summary>
        /// Determine if the data provided is a master
        /// </summary>
        public override bool IsMaster(TModel entity)
        {
            if(entity == default(TModel))
            {
                throw new ArgumentNullException(nameof(entity), "Entity argument null");
            }
            if (entity.GetTag(MdmConstants.MdmTypeTag) == "M")
            {
                return true;
            }
            else if (entity.ClassConceptKey == MdmConstants.MasterRecordClassification)
            {
                return true;
            }
            else if (entity.Key.HasValue)
            {
                return this.m_entityPersistenceService.Get(entity.Key.Value, null, true, AuthenticationContext.SystemPrincipal)?.ClassConceptKey == MdmConstants.MasterRecordClassification;
            }
            return false;
        }

        /// <summary>
        /// Get local for specified object
        /// </summary>
        public override TModel GetLocalFor(Guid masterKey, IPrincipal principal)
        {
            IIdentity identity = null;
            if (principal is IClaimsPrincipal claimsPrincipal)
            {
                identity = claimsPrincipal?.Identities.OfType<IDeviceIdentity>().FirstOrDefault() as IIdentity ??
                            claimsPrincipal?.Identities.OfType<IApplicationIdentity>().FirstOrDefault() as IIdentity;
            }
            else
            {
                identity = principal.Identity;
            }

            TModel retVal = null;
            // Identity
            if (identity is IDeviceIdentity deviceIdentity)
                return this.m_persistenceService.Query(o => o.Relationships.Where(g => g.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Any(g => g.TargetEntityKey == masterKey) && o.CreatedBy.Device.Name == deviceIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
            else if (identity is IApplicationIdentity applicationIdentity)
                return this.m_persistenceService.Query(o => o.Relationships.Where(g => g.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Any(g => g.TargetEntityKey == masterKey) && o.CreatedBy.Application.Name == applicationIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
            else
                return null;
        }


        /// <summary>
        /// Create a new local record for <paramref name="masterRecord"/>
        /// </summary>
        public override TModel CreateLocalFor(TModel masterRecord)
        {
            var retVal = new TModel();
            retVal.Key = Guid.NewGuid();
            retVal.VersionKey = Guid.NewGuid();
            retVal.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
            retVal.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, masterRecord)
            {
                ClassificationKey = MdmConstants.VerifiedClassification
            });
            retVal.Relationships.Where(o => o.SourceEntityKey == masterRecord.Key).ToList().ForEach(r => r.SourceEntityKey = null);
            return retVal;
        }

        /// <summary>
        /// Promote to record of truth
        /// </summary>
        public override TModel PromoteRecordOfTruth(TModel local)
        {
            var master = (Entity)this.GetRawMaster(local);
            if (master == null)
            {
                throw new InvalidOperationException($"Cannot find Master for {local}");
            }
            local.DeterminerConceptKey = MdmConstants.RecordOfTruthDeterminer;

            var rotRelationship = local.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship) ??
                master.LoadCollection(o => o.Relationships).SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);

            if (rotRelationship == null)
            {
                rotRelationship = new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, local);
                local.Relationships.Add(rotRelationship);
            }

            rotRelationship.SourceEntityKey = master.Key;
            return local;
        }

        /// <summary>
        /// Determine if the entity is already a ROT or wants to be
        /// </summary>
        public override bool IsRecordOfTruth(TModel entity)
        {
            if (entity.GetTag(MdmConstants.MdmTypeTag) == "T")
            {
                return true;
            }
            else if (entity.Key.HasValue)
            {
                return this.m_entityPersistenceService.Get(entity.Key.Value, null, true, AuthenticationContext.SystemPrincipal)?.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer;
            }
            return false;
        }

        /// <summary>
        /// Get master for the specified object
        /// </summary>
        public override IdentifiedData GetRawMaster(TModel local)
        {
            return this.GetMasterFor(local, null);
        }

        /// <summary>
        /// Get master for specified object within a context
        /// </summary>
        public Entity GetMasterFor(TModel local, IEnumerable<IdentifiedData> context)
        {
            if (this.IsMaster(local))
            {
                return local;
            }
            else
            {
                var masRel = this.GetMasterRelationshipFor(local, context);
                if (masRel != null)
                {
                    return context?.OfType<Entity>().FirstOrDefault(o => o.Key == masRel.TargetEntityKey) ??
                        masRel.LoadProperty(o => o.TargetEntity);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Get master relationship for specified local
        /// </summary>
        private EntityRelationship GetMasterRelationshipFor(Guid? localKey, IEnumerable<IdentifiedData> context)
        {
            if (!localKey.HasValue)
            {
                return null;
            }

            var masRel = context?.OfType<EntityRelationship>().FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordClassification && o.SourceEntityKey == localKey);
            if (masRel != null)
            {
                return masRel;
            }
            else
            {
                return this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.SourceEntityKey == localKey && o.ObsoleteVersionSequenceId == null, 0, 1, out int _, AuthenticationContext.SystemPrincipal).FirstOrDefault();
            }
        }
        /// <summary>
        /// Get master relationship for specified local
        /// </summary>
        private EntityRelationship GetMasterRelationshipFor(TModel local, IEnumerable<IdentifiedData> context)
        {
            return local.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship)
                ?? this.GetMasterRelationshipFor(local.Key, context);
        }

        /// <summary>
        /// Extract relationships of note for the MDM layer
        /// </summary>
        public override IEnumerable<ISimpleAssociation> ExtractRelationships(TModel store)
        {
            var retVal = store.Relationships.Where(o => o.SourceEntityKey != store.Key).ToList();
            store.Relationships.RemoveAll(o => o.SourceEntityKey != store.Key);
            return retVal;
        }

        /// <summary>
        /// Refactor relationships
        /// </summary>
        public override void RefactorRelationships(List<IdentifiedData> item, Guid fromEntityKey, Guid toEntityKey)
        {
            foreach (var obj in item)
            {
                if (obj is Entity entity)
                {
                    entity.Relationships.Where(o => o.SourceEntityKey == fromEntityKey).ToList().ForEach(o => o.SourceEntityKey = toEntityKey);
                    entity.Relationships.Where(o => o.TargetEntityKey == fromEntityKey).ToList().ForEach(o => o.TargetEntityKey = toEntityKey);
                }
                else if (obj is Act act)
                {
                    act.Participations.Where(o => o.PlayerEntityKey == fromEntityKey).ToList().ForEach(o => o.PlayerEntityKey = toEntityKey);
                }
                else if (obj is ITargetedAssociation entityRelationship)
                {
                    if (entityRelationship.SourceEntityKey == fromEntityKey)
                    {
                        entityRelationship.SourceEntityKey = toEntityKey;
                    }
                    if (entityRelationship.TargetEntityKey == fromEntityKey)
                    {
                        entityRelationship.TargetEntityKey = toEntityKey;
                    }
                }
            }
        }

        /// <summary>
        /// Validate the MDM state
        /// </summary>
        public override IEnumerable<DetectedIssue> ValidateMdmState(TModel data)
        {
            if (data.ClassConceptKey != MdmConstants.MasterRecordClassification)
            {
                if (data.LoadCollection(o => o.Relationships).Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordClassification && r.ObsoleteVersionSequenceId == null) != 1)
                {
                    yield return new DetectedIssue(DetectedIssuePriorityType.Error, "MDM-ORPHAN", $"{data} appears to be an MDM Orphan", DetectedIssueKeys.FormalConstraintIssue);
                }

            }
        }

        /// <summary>
        /// Synthesize the specified query
        /// </summary>
        public override IEnumerable<IMdmMaster> MdmQuery(NameValueCollection masterQuery, NameValueCollection localQuery, Guid? queryId, int offset, int? count, out int totalResults)
        {
            var localLinq = QueryExpressionParser.BuildLinqExpression<Entity>(localQuery, null, false);

            // Try to do a linked query (unless the query is on a special local filter value)
            // TODO: Make it configurable which properties trigger a master query
            if (masterQuery.Keys.Any(o => o.StartsWith("identifier")) && this.m_entityPersistenceService is IUnionQueryDataPersistenceService<Entity> unionQuery)
            {
                var masterLinq = QueryExpressionParser.BuildLinqExpression<Entity>(masterQuery, null, false);
                return unionQuery.Union(new Expression<Func<Entity, bool>>[] { localLinq, masterLinq }, queryId.GetValueOrDefault(), offset, count, out totalResults, AuthenticationContext.SystemPrincipal).Select(this.Synthesize);
            }
            else if (this.m_entityPersistenceService is IStoredQueryDataPersistenceService<Entity> storedQuery)
            {
                return storedQuery.Query(localLinq, queryId.GetValueOrDefault(), offset, count ?? 100, out totalResults, AuthenticationContext.SystemPrincipal).Select(this.Synthesize);
            }
            else
                return this.m_entityPersistenceService.Query(localLinq, offset, count ?? 100, out totalResults, AuthenticationContext.SystemPrincipal).Select(this.Synthesize);
        }

        /// <summary>
        /// Synthesize a master
        /// </summary>
        private IMdmMaster Synthesize(Entity masterResult)
        {
            return new EntityMaster<TModel>(masterResult);
        }

        /// <summary>
        /// Get the master
        /// </summary>
        public override IMdmMaster MdmGet(Guid masterKey)
        {
            var master = this.m_entityPersistenceService.Get(masterKey, null, false, AuthenticationContext.SystemPrincipal);
            if (master != null)
            {
                return new EntityMaster<TModel>(master);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Return true if <paramref name="principal"/> is the owner of <paramref name="data"/>
        /// </summary>
        public override bool IsOwner(TModel data, IPrincipal principal)
        {
            var provenanceApp = data.LoadProperty(o => o.CreatedBy)?.LoadProperty(o => o.Application);
            if (principal is IClaimsPrincipal claimsPrincipal)
            {
                var applicationPrincipal = claimsPrincipal.Identities.OfType<IApplicationIdentity>().SingleOrDefault();
                return provenanceApp.Name == applicationPrincipal.Name;
            }
            else
            {
                return provenanceApp.Name == principal.Identity.Name;
            }
        }

        /// <summary>
        /// Establish master for the specified object
        /// </summary>
        public override IdentifiedData EstablishMasterFor(TModel local)
        {
            var retVal = new EntityMaster<TModel>()
            {
                Key = Guid.NewGuid(),
                VersionKey = null,
                CreatedByKey = Guid.Parse(AuthenticationContext.SystemApplicationSid),
                DeterminerConceptKey = DeterminerKeys.Specific
            };
            local.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, local.Key, retVal.Key, MdmConstants.AutomagicClassification));
            return retVal;
        }

        /// <summary>
        /// Obsolete the specified object
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxObsolete(TModel data, IEnumerable<IdentifiedData> context)
        {
            // First we get the master
            var master = (Entity)this.GetRawMaster(data);

            // How many other relationships are on this master which aren't us and active?
            // No other active?
            if (this.m_relationshipService.Count(o => o.TargetEntityKey != data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.SourceEntityKey == master.Key && o.ObsoleteVersionSequenceId == null) == 0)
            {
                master.StatusConceptKey = StatusKeys.Obsolete;
                yield return master; // ensure master is obsoleted
            }
            data.StatusConceptKey = StatusKeys.Obsolete;
            yield return data; // ensure data is obsoleted
        }

        /// <summary>
        /// Save the record of truth
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxSaveRecordOfTruth(TModel data, IEnumerable<IdentifiedData> context)
        {
            var retVal = new LinkedList<IdentifiedData>(context);

            // Ensure that data is appropriately valid for ROT
            if (data.GetTag(MdmConstants.MdmTypeTag) != "T")
            {
                data.AddTag(MdmConstants.MdmTypeTag, "T");
            }

            var master = this.GetMasterFor(data, context);
            if (master == null)
            {
                master = (Entity)this.EstablishMasterFor(data);
                retVal.AddLast(master);
            }

            // Ensure that ROT points to a master
            var rotRel = context.OfType<EntityRelationship>().FirstOrDefault(o => o.TargetEntityKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship) ??
                data.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.TargetEntityKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
            if (rotRel == null)
            {
                retVal.AddLast(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, data.Key) { SourceEntityKey = master.Key });
            }
            else if (rotRel.SourceEntityKey != master.Key || rotRel.TargetEntityKey != data.Key) // ROT rel has changed
            {
                rotRel.ObsoleteVersionSequenceId = Int32.MaxValue;
                retVal.AddLast(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, data.Key) { SourceEntityKey = master.Key });
            }

            // Persist master in the transaction?
            if (!context.Any(r => r.Key == master.Key))
            {
                retVal.AddFirst(master);
            }
            if (!context.Any(r => r.Key == data.Key))
            {
                retVal.AddFirst(data);
            }

            return context;
        }

        /// <summary>
        /// Save local 
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxSaveLocal(TModel data, IEnumerable<IdentifiedData> context)
        {

            // Return value
            var retVal = new LinkedList<IdentifiedData>(context);

            // Validate that we can store this in context
            data.Tags.RemoveAll(t => t.TagKey == MdmConstants.MdmTypeTag);

            // First, we want to perform a match 
            var matchInstructions = this.MdmTxMatchMasters(data, context).ToArray();

            // Persist master in the transaction?
            if (!context.Any(r => r.Key == data.Key))
            {
                retVal.AddFirst(data);

                // we need to remove any MDM relationships from the object since they'll be in the tx
                data.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship ||
                    o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship ||
                    o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship);
                // Add them from the match instructions
                data.Relationships.AddRange(matchInstructions.OfType<EntityRelationship>().Where(o => o.SourceEntityKey == data.Key));

            }


            // Match instructions
            foreach (var rv in matchInstructions)
            {
                retVal.AddLast(rv);
            }

            return retVal;
        }

        /// <summary>
        /// Match masters
        /// </summary>
        /// <remarks>
        /// This procedure will reach out to the matching engine configured and will perform
        /// matching. This matching is done via identifier or fuzzy match. The function will 
        /// then prepare a series of steps which need to be performed so that the persistence
        /// layer can update the relationships to match the master.
        /// 
        /// If <paramref name="context"/> is passed, it indicates that the match is happening in the context
        /// of a transaction and that the <paramref name="context"/> data should be taken into consideration.
        /// 
        /// The <paramref name="context"/> are not add back into the result.
        /// </remarks>
        public override IEnumerable<IdentifiedData> MdmTxMatchMasters(TModel local, IEnumerable<IdentifiedData> context)
        {
            // Return value
            LinkedList<IdentifiedData> retVal = new LinkedList<IdentifiedData>();
            bool rematchMaster = false;

            var existingMasterRel = this.GetMasterRelationshipFor(local, context);
            if (existingMasterRel != null)
            {
                if (!existingMasterRel.Key.HasValue)
                {
                    existingMasterRel = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.TargetEntityKey == existingMasterRel.TargetEntityKey && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, 0, 1, out int _, AuthenticationContext.SystemPrincipal).FirstOrDefault() ?? existingMasterRel;
                }
                retVal.AddLast(existingMasterRel);
                rematchMaster = this.m_relationshipService.Count(r=> r.TargetEntityKey == existingMasterRel.TargetEntityKey && r.SourceEntityKey != local.Key && r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.ObsoleteVersionSequenceId == null) > 0; // we'll need to rematch

                // Remove any references to MDM controlled objects in the actual object itself - they'll be used in the TX bundle
                local.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship ||
                    o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship ||
                    o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship);
            }

            // Get the ignore list
            var ignoreList = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship, AuthenticationContext.SystemPrincipal).Select(o => o.TargetEntityKey.Value)
                .Union(context.OfType<EntityRelationship>().Where(o => o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship).Select(o => o.TargetEntityKey.Value)).ToArray();

            // Existing probable links and set them to obsolete for now
            var existingCandidates = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship, AuthenticationContext.SystemPrincipal);
            foreach (var pl in existingCandidates)
            {
                pl.ObsoleteVersionSequenceId = Int32.MaxValue;
                retVal.AddLast(pl);
            }

            // Match configuration
            // TODO: Emit logs
            foreach (var cnf in this.m_resourceConfiguration.MatchConfiguration)
            {
                // Get a list of match results
                var matchResults = this.m_matchingService.Match<TModel>(local, cnf.MatchConfiguration, ignoreList);

                // Group the match results by their outcome
                var matchResultGrouping = matchResults
                    .Where(o => o.Record.Key != local.Key) // cannot match with itself
                    .Select(o => new MasterMatch(this.GetMasterFor(o.Record, context).Key.Value, o))
                    .GroupBy(o => o.MatchResult.Classification)
                    .ToDictionary(o => o.Key, o => o.Distinct());

                // Ensure we have both match and nonmatch
                if (!matchResultGrouping.ContainsKey(RecordMatchClassification.Match))
                {
                    matchResultGrouping.Add(RecordMatchClassification.Match, new MasterMatch[0]);
                }
                if (!matchResultGrouping.ContainsKey(RecordMatchClassification.Probable))
                {
                    matchResultGrouping.Add(RecordMatchClassification.Probable, new MasterMatch[0]);
                }
                matchResultGrouping.Remove(RecordMatchClassification.NonMatch);

                // IF MATCHES.COUNT == 1 AND AUTOLINK = TRUE
                if (matchResultGrouping[RecordMatchClassification.Match].Count() == 1 &&
                    cnf.AutoLink)
                {
                    var matchedMaster = matchResultGrouping[RecordMatchClassification.Match].Single();
                    if (existingMasterRel == null) // There is no master, so we can just like
                    {
                        retVal.AddLast(new EntityRelationship(MdmConstants.MasterRecordRelationship, local.Key, matchedMaster.Master, MdmConstants.AutomagicClassification)
                        { Strength = matchedMaster.MatchResult.Strength }
                        );
                    }
                    // The matching engine wants to change the master link
                    else if (matchedMaster.Master != existingMasterRel.TargetEntityKey)
                    {
                        // Old master was verified, so we don't touch it we just suggest a link
                        if (existingMasterRel.ClassificationKey == MdmConstants.VerifiedClassification)
                        {
                            rematchMaster = false;
                            retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, matchedMaster.Master, MdmConstants.AutomagicClassification)
                            {
                                Strength = matchedMaster.MatchResult.Strength
                            });
                        }
                        else // old master was not verified, so we re-link
                        {
                            var mdmMatchInstructions = this.MdmTxMasterLink(matchedMaster.Master, local.Key.Value, context.Union(retVal), false);
                            foreach (var itm in mdmMatchInstructions)
                            {
                                retVal.AddLast(itm);
                                if (itm.SemanticEquals(existingMasterRel))
                                {
                                    existingMasterRel.SemanticCopy(itm);
                                }
                            }
                            
                        }
                    }
                    else
                    {
                        rematchMaster = false; // same master so no need to rematch
                    }
                }
                // IF MATCHES.COUNT > 1 OR AUTOLINK = FALSE
                else if (!cnf.AutoLink || matchResultGrouping[RecordMatchClassification.Match].Count() > 1)
                {
                    // Create as candidates for non-existing master
                    var nonMasterLinks = matchResultGrouping[RecordMatchClassification.Match].Where(o => o.Master != existingMasterRel?.TargetEntityKey);
                    foreach (var nml in nonMasterLinks)
                    {
                        retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, nml.Master, MdmConstants.AutomagicClassification) { Strength = nml.MatchResult.Strength });
                    }
                }

                // Candidate matches
                var nonMasterCandidates = matchResultGrouping[RecordMatchClassification.Probable].Where(o => o.Master != existingMasterRel?.TargetEntityKey);
                foreach (var nmc in nonMasterCandidates)
                {
                    retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, nmc.Master, MdmConstants.AutomagicClassification) { Strength = nmc.MatchResult.Strength });
                }
            }

            // Is the existing master rel still in place?
            if (rematchMaster)
            {
                var masterDetail = this.MdmGet(existingMasterRel.TargetEntityKey.Value).GetMaster(AuthenticationContext.SystemPrincipal) as TModel;
                var bestMatch = this.m_resourceConfiguration.MatchConfiguration.SelectMany(c => this.m_matchingService.Classify(local, new TModel[] { masterDetail }, c.MatchConfiguration)).OrderByDescending(o => o.Classification).FirstOrDefault();
                switch (bestMatch.Classification)
                // No longer a match
                {
                    case RecordMatchClassification.Probable:
                        // Is the existing "VERIFIED"
                        if (existingMasterRel.ClassificationKey == MdmConstants.VerifiedClassification)
                        {
                            // This means that other records on the existing master are "evicted" if they're not verified
                            var nonVerifiedLocals = this.m_relationshipService.Query(o => o.ClassificationKey != MdmConstants.VerifiedClassification && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.TargetEntityKey == masterDetail.Key && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
                            Entity newMaster = null;
                            foreach(var oldLocal in nonVerifiedLocals)
                            {
                                if(newMaster == null)
                                {
                                    newMaster = (Entity)this.EstablishMasterFor((TModel)oldLocal.LoadProperty(o => o.SourceEntity));
                                    yield return newMaster;
                                }
                                oldLocal.ObsoleteVersionSequenceId = Int32.MaxValue;
                                yield return oldLocal;
                                yield return new EntityRelationship(MdmConstants.OriginalMasterRelationship, oldLocal.SourceEntityKey, oldLocal.TargetEntityKey, MdmConstants.AutomagicClassification);
                                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, oldLocal.SourceEntityKey, newMaster.Key, MdmConstants.AutomagicClassification);
                            }
                        }
                        else
                        {
                            existingMasterRel.ObsoleteVersionSequenceId = Int32.MaxValue;
                            retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength });
                            retVal.AddLast(new EntityRelationship(MdmConstants.OriginalMasterRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength });
                        }
                        break;
                    case RecordMatchClassification.NonMatch:
                        existingMasterRel.ObsoleteVersionSequenceId = Int32.MaxValue;
                        retVal.AddLast(new EntityRelationship(MdmConstants.OriginalMasterRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength });
                        break;
                    case RecordMatchClassification.Match:
                        break;
                }
            }
            // Is there no master link?
            if (!retVal.OfType<EntityRelationship>().Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.ObsoleteVersionSequenceId == null))
            {
                // Return a master at the top of the return list
                yield return this.EstablishMasterFor(local);
                retVal.AddLast(local.Relationships.SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
            }

            // Clean up entities by their target so that :
            // 1. Only one relationship between LOCAL and TARGET exist
            // 2. The most strong link is the persisted link
            // 3. If an obsolete was overwritten it is taken as the 
            // We do this so the database doesn't become overwhelmed with churn from relationships being rewritten
            foreach (var res in retVal.OfType<EntityRelationship>().GroupBy(o => o.TargetEntityKey))
            {
                // Definite matches to master which are not to be deleted
                var masterRelationships = res.Where(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).OrderByDescending(o => o.Strength);
                var candidateRelationships = res.Where(o => o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship).OrderByDescending(o => o.Strength);
                var originalRelationships = res.Where(o => o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship).OrderByDescending(o => o.Strength);
                
                // If all the master relationships between source and target are to be removed so remove it
                if (masterRelationships.Any() && masterRelationships.All(o => o.ObsoleteVersionSequenceId.HasValue))
                {
                    yield return masterRelationships.First(); // Return 
                    if (originalRelationships.Any()) // There is an original relationship so send that back
                        yield return originalRelationships.First();
                    if (candidateRelationships.Any(r => !r.ObsoleteVersionSequenceId.HasValue)) // There is a candidate which is active so send that back
                        yield return candidateRelationships.FirstOrDefault(o => !o.ObsoleteVersionSequenceId.HasValue);
                }
                // There is a master to be deleted but not all of them (i.e. there is an active one between L and M)
                // so we just want to keep the current active
                else if (masterRelationships.Any(o => o.ObsoleteVersionSequenceId.HasValue) && masterRelationships.Any(o => !o.ObsoleteVersionSequenceId.HasValue))
                {
                    var masterRel = masterRelationships.First(o => o.ObsoleteVersionSequenceId.HasValue);
                    masterRel.ObsoleteVersionSequenceId = null; // Don't delete it
                    masterRel.Strength = masterRelationships.First(o => !o.ObsoleteVersionSequenceId.HasValue).Strength;
                    yield return masterRel;
                }
                else if (masterRelationships.Any(o => !o.ObsoleteVersionSequenceId.HasValue)) // There's a master relationship which is new and not to be deleted
                {
                    yield return masterRelationships.First();
                    if (candidateRelationships.Any(r => r.ObsoleteVersionSequenceId.HasValue)) // There is a candidate 
                        yield return candidateRelationships.FirstOrDefault(o => o.ObsoleteVersionSequenceId.HasValue);
                }
                // If there is a candidate that is marked as to be deleted
                // but another which is not - we take the candidate with a current
                // key and update the strength (i.e. the candidate still is valid)
                else if (candidateRelationships.Any(r => r.Key.HasValue) && !candidateRelationships.All(r => r.ObsoleteVersionSequenceId.HasValue))
                {
                    var existingRel = candidateRelationships.FirstOrDefault(o => o.ObsoleteVersionSequenceId.HasValue); // the obsoleted one already exists in DB
                    existingRel.Strength = candidateRelationships.First().Strength;
                    existingRel.ObsoleteVersionSequenceId = null;
                    yield return existingRel;
                }
                else if(candidateRelationships.Any())
                {
                    yield return candidateRelationships.FirstOrDefault(); // Most strong link
                }

                // Other relationship types
                foreach (var otherRel in res.Where(o => MdmConstants.MasterRecordRelationship != o.RelationshipTypeKey && MdmConstants.CandidateLocalRelationship != o.RelationshipTypeKey && MdmConstants.OriginalMasterRelationship != o.RelationshipTypeKey))
                {
                    yield return otherRel;
                }
            }

            // return non-er relationships
            foreach(var itm in retVal)
            {
                if(!(itm is EntityRelationship))
                {
                    yield return itm;
                }
            }

        }

        /// <summary>
        /// Perform necessary steps to link <paramref name="masterKey"/> and <paramref name="localKey"/> as a master
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxMasterLink(Guid masterKey, Guid localKey, IEnumerable<IdentifiedData> context, bool verified)
        {
            if (this.IsMaster(masterKey))
            {
                if (this.IsMaster(localKey))
                {
                    throw new InvalidOperationException("Cannot link MASTER to MASTER");
                }

                var existingRelationship = this.GetMasterRelationshipFor(localKey, context);

                // Obsolete the existing
                if (existingRelationship != null)
                {
                    if (existingRelationship.TargetEntityKey != masterKey)
                    {
                        existingRelationship.ObsoleteVersionSequenceId = Int32.MaxValue;
                        yield return existingRelationship;
                        if (!verified) // store link to original
                        {
                            yield return new EntityRelationship(MdmConstants.OriginalMasterRelationship, localKey, existingRelationship.TargetEntityKey, MdmConstants.AutomagicClassification);
                        }

                        // Recheck the original master for any other links
                        var dbRels = this.m_relationshipService.Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == existingRelationship.TargetEntityKey && r.SourceEntityKey != localKey && r.ObsoleteVersionSequenceId == null) +
                            context.OfType<EntityRelationship>().Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == existingRelationship.TargetEntityKey && r.SourceEntityKey != localKey && r.ObsoleteVersionSequenceId == null);
                        if(dbRels == 0)
                        {
                            var oldMasterRec = context.OfType<Entity>().FirstOrDefault(o => o.Key == existingRelationship.TargetEntityKey) ??
                                this.GetRaw(existingRelationship.TargetEntityKey.Value) as Entity;
                            oldMasterRec.StatusConceptKey = StatusKeys.Obsolete;
                            yield return oldMasterRec;
                            yield return new EntityRelationship(EntityRelationshipTypeKeys.Replaces, masterKey, oldMasterRec.Key, MdmConstants.AutomagicClassification);
                        }

                    }
                    else
                    {
                        if (existingRelationship.ClassificationKey == MdmConstants.AutomagicClassification && verified)
                        {
                            existingRelationship.ClassificationKey = MdmConstants.VerifiedClassification;
                        }
                        yield return existingRelationship;
                        yield break;
                    }
                }
                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, localKey, masterKey, verified ? MdmConstants.VerifiedClassification : MdmConstants.AutomagicClassification);
            }
            else
            {
                if (!this.IsMaster(localKey))
                {
                    throw new InvalidOperationException("Cannot link LOCAL to non-MASTER");
                }

                // User had the rel backwards
                foreach (var itm in this.MdmTxMasterLink(localKey, masterKey, context, verified))
                    yield return itm;
            }
        }

        /// <summary>
        /// Unlink a previously established link
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxMasterUnlink(Guid fromKey, Guid toKey, IEnumerable<IdentifiedData> context)
        {
            if (context == null)
            {
                context = new IdentifiedData[0]; // revent nre
            }

            if (this.IsMaster(fromKey))
            {

                var existingRelationship = this.GetMasterRelationshipFor(toKey, null);
                if (existingRelationship == null || existingRelationship.TargetEntityKey != fromKey)
                {
                    throw new InvalidOperationException($"Cannot unlink {toKey} from {fromKey} as MDM relationship in place is between {existingRelationship.SourceEntityKey} and {existingRelationship.TargetEntityKey}");
                }

                // First, add an ignore instruction
                existingRelationship.RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship;
                existingRelationship.ClassificationKey = MdmConstants.VerifiedClassification;
                yield return existingRelationship;

                // Next, establsh a new MDM master
                foreach (var itm in this.MdmTxMatchMasters((TModel)existingRelationship.LoadProperty(o => o.SourceEntity), new IdentifiedData[] { existingRelationship }))
                {
                    yield return itm;
                }
            }
            else
            {

                var existingRelationship = this.GetMasterRelationshipFor(fromKey, null);
                if (existingRelationship == null || existingRelationship.TargetEntityKey != toKey)
                {
                    throw new InvalidOperationException($"Cannot unlink {fromKey} from {toKey} as MDM relationship in place is between {existingRelationship.SourceEntityKey} and {existingRelationship.TargetEntityKey}");
                }

                // First, add an ignore instruction
                existingRelationship.RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship;
                existingRelationship.ClassificationKey = MdmConstants.VerifiedClassification;
                yield return existingRelationship;

                // Next, establsh a new MDM master
                foreach (var itm in this.MdmTxMatchMasters((TModel)existingRelationship.LoadProperty(o => o.SourceEntity), context.Union(new IdentifiedData[] { existingRelationship })))
                {
                    yield return itm;
                }
            }
        }
        /// <summary>
        /// Create transaction instructions to ignore future matches between <paramref name="hostKey"/> and <paramref name="ignoreKey"/>
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, IEnumerable<IdentifiedData> context)
        {
            if (this.IsMaster(hostKey))
            {
                yield return new EntityRelationship()
                {
                    SourceEntityKey = ignoreKey,
                    TargetEntityKey = hostKey,
                    ClassificationKey = MdmConstants.VerifiedClassification,
                    RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship
                };
            }
            else
            {
                yield return new EntityRelationship()
                {
                    SourceEntityKey = hostKey,
                    TargetEntityKey = ignoreKey,
                    ClassificationKey = MdmConstants.VerifiedClassification,
                    RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship
                };
            }
        }

        /// <summary>
        /// Gets the local associations (the locals) attached to the master key
        /// </summary>
        public override IEnumerable<ITargetedAssociation> GetAssociatedLocals(Guid masterKey)
        {
            return this.m_relationshipService.Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Gets the candidate locals of a specified master
        /// </summary>
        /// <param name="masterKey">The master key to fetch</param>
        /// <returns>The candidate locals which have not been established</returns>
        public override IEnumerable<ITargetedAssociation> GetCandidateLocals(Guid masterKey)
        {
            return this.m_relationshipService.Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Get all candidates which should be ignored
        /// </summary>
        public override IEnumerable<ITargetedAssociation> GetIgnoredCandidateLocals(Guid masterKey)
        {
            return this.m_relationshipService.Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Get all candidate locals
        /// </summary>
        public override IEnumerable<IdentifiedData> GetAllMdmCandidateLocals()
        {
            return this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Get the master construct record for <paramref name="localKey"/>
        /// </summary>
        public override IMdmMaster GetMasterFor(Guid localKey)
        {
            var masterEntity = this.m_entityPersistenceService.Get(localKey, null, true, AuthenticationContext.SystemPrincipal) as Entity;
            if (masterEntity.ClassConceptKey == MdmConstants.MasterRecordClassification)
            {
                return new EntityMaster<TModel>(masterEntity);
            }
            else
            {
                // This is a local find the master
                masterEntity = this.GetMasterRelationshipFor((TModel)masterEntity, null).LoadProperty(o => o.TargetEntity);
                return new EntityMaster<TModel>(masterEntity);
            }
        }

        /// <summary>
        /// Get candidate master established for this local
        /// </summary>
        public override IEnumerable<ITargetedAssociation> GetEstablishedCandidateMasters(Guid localKey)
        {
            return this.m_relationshipService.Query(o => o.SourceEntityKey == localKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Get all ignored masters
        /// </summary>
        public override IEnumerable<ITargetedAssociation> GetIgnoredMasters(Guid localKey)
        {
            return this.m_relationshipService.Query(o => o.SourceEntityKey == localKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal);
        }

        /// <summary>
        /// Create transaction instructiosn to merge <paramref name="victimKey"/> into <paramref name="survivorKey"/>
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxMergeMasters(Guid survivorKey, Guid victimKey, IEnumerable<IdentifiedData> context)
        {
            // First enusre validate state
            if (!this.IsMaster(survivorKey) || !this.IsMaster(victimKey))
            {
                throw new InvalidOperationException($"Moth {survivorKey} and {victimKey} must be MASTER");
            }

            // First we obsolete the old
            var survivorData = this.GetRaw(survivorKey) as Entity;
            var victimData = this.GetRaw(victimKey) as Entity;
            victimData.StatusConceptKey = StatusKeys.Obsolete;
            yield return victimData;

            // Associated locals for the victim are mapped
            foreach (var rel in this.GetAssociatedLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.ObsoleteVersionSequenceId = Int32.MaxValue;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey)
                {
                    Strength = rel.Strength
                };
            }

            // Associated candidates are mapped
            foreach (var rel in this.GetIgnoredCandidateLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.ObsoleteVersionSequenceId = Int32.MaxValue;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.IgnoreCandidateRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey);
            }

            // Associated matches are mapped 
            foreach (var rel in this.GetCandidateLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.ObsoleteVersionSequenceId = Int32.MaxValue;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.CandidateLocalRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey);
            }

            // Identifiers for the victim are migrated
            foreach (var ident in victimData.LoadCollection(o => o.Identifiers).Where(o => !survivorData.LoadCollection(s => s.Identifiers).Any(s => !s.SemanticEquals(o))))
            {
                yield return new EntityIdentifier(ident.Authority, ident.Value)
                {
                    IssueDate = ident.IssueDate,
                    SourceEntityKey = survivorKey
                };
            }

        }

        /// <summary>
        /// Get all associations related to MDM 
        /// </summary>
        public override IEnumerable<ITargetedAssociation> GetAllMdmAssociations(Guid localKey)
        {
            if(this.m_relationshipService is IUnionQueryDataPersistenceService<EntityRelationship> iups)
            {
                return
                    iups.Union(new Expression<Func<EntityRelationship, bool>>[] {
                        o => (o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship) && o.ObsoleteVersionSequenceId == null && o.SourceEntityKey == localKey,
                        o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null && o.TargetEntityKey == localKey
                    }, Guid.Empty, 0, 100, out int _, AuthenticationContext.SystemPrincipal);
            }
            else
            {
                return
                    this.m_relationshipService.Query(
                        o => (o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship) && o.ObsoleteVersionSequenceId == null && o.SourceEntityKey == localKey, AuthenticationContext.SystemPrincipal)
                        .Union(this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null && o.TargetEntityKey == localKey, AuthenticationContext.SystemPrincipal));
            }
        }
    }
}
