﻿/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using SanteDB.Core;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.i18n;
using SanteDB.Core.Matching;
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
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Security.Principal;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Data manager for MDM services for entities
    /// </summary>
    public class MdmEntityDataManager<TModel> : MdmDataManager<TModel>
        where TModel : Entity, new()
    {

        // Tracer
        private readonly Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // Entity persistence serviuce
        private IDataPersistenceService<Entity> m_entityPersistenceService;

        // entity tag persistence
        private IDataPersistenceService<EntityExtension> m_entityTagPersistenceService;

        // Policy enforcement service
        private IPolicyEnforcementService m_policyEnforcement;

        // Persistence service
        private IDataPersistenceService<TModel> m_persistenceService;

        // Relationship service
        private IDataPersistenceService<EntityRelationship> m_relationshipService;

        // Matching service
        private IRecordMatchingService m_matchingService;

        // Matching configuration service
        private IRecordMatchingConfigurationService m_matchingConfigurationService;

        /// <summary>
        /// Create entity data manager
        /// </summary>
        public MdmEntityDataManager() : base(ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>() as IDataPersistenceService)
        {
            ModelSerializationBinder.RegisterModelType(typeof(EntityMaster<TModel>));
            ModelSerializationBinder.RegisterModelType($"{typeof(TModel).Name}Master", typeof(EntityMaster<TModel>));

            this.m_entityTagPersistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityExtension>>();
            this.m_policyEnforcement = ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>(); ;
            this.m_matchingConfigurationService = ApplicationServiceContext.Current.GetService<IRecordMatchingConfigurationService>(); ;
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
            return this.m_entityPersistenceService.Query(o => o.Key == dataKey && o.ClassConceptKey == MdmConstants.MasterRecordClassification && (o.ObsoletionTime == null || o.ObsoletionTime != null), AuthenticationContext.SystemPrincipal).Any();
        }

        /// <summary>
        /// Determine if the data provided is a master
        /// </summary>
        public override bool IsMaster(TModel entity)
        {
            if (entity == default(TModel))
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
            // The user may be promoting a master setting the determiner concept key
            else if (entity.Key.HasValue && (entity.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer || !entity.ClassConceptKey.HasValue))
            {
                return this.m_entityPersistenceService.Get(entity.Key.Value, null, AuthenticationContext.SystemPrincipal)?.ClassConceptKey == MdmConstants.MasterRecordClassification;
            }
            return false;
        }

        /// <summary>
        /// Get local for specified object
        /// </summary>
        public override IdentifiedData GetLocalFor(Guid masterKey, IPrincipal principal)
        {
            IIdentity identity = null;
            if (principal is IClaimsPrincipal claimsPrincipal)
            {
                identity = claimsPrincipal?.Identities.OfType<IDeviceIdentity>().FirstOrDefault() as IIdentity ??
                            claimsPrincipal?.Identities.OfType<IApplicationIdentity>().FirstOrDefault();
            }
            else
            {
                identity = principal.Identity;
            }

            // Identity
            if (identity is IDeviceIdentity deviceIdentity)
            {
                return this.m_persistenceService.Query(o => o.Relationships.Where(g => g.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Any(g => g.TargetEntityKey == masterKey) && o.CreationAct.CreatedBy.Device.Name == deviceIdentity.Name, AuthenticationContext.SystemPrincipal).FirstOrDefault();
            }
            else if (identity is IApplicationIdentity applicationIdentity)
            {
                return this.m_persistenceService.Query(o => o.Relationships.Where(g => g.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Any(g => g.TargetEntityKey == masterKey) && o.CreationAct.CreatedBy.Application.Name == applicationIdentity.Name, AuthenticationContext.SystemPrincipal).FirstOrDefault();
            }
            else
            {
                return null;
            }
        }

        /// <inheritdoc/>
        public override IdentifiedData CreateLocalFor(IdentifiedData masterRecord)
        {
            if (masterRecord is TModel tm)
            {
                return this.CreateLocalFor(tm);
            }
            if (masterRecord is Entity ent &&
                ent.ClassConceptKey == MdmConstants.MasterRecordClassification)
            {
                var retVal = new TModel();
                Guid? originalClass = retVal.ClassConceptKey, originalDeterminer = retVal.DeterminerConceptKey;
                retVal.SemanticCopyNullFields(masterRecord);
                retVal.SemanticCopyNullFields(masterRecord); // HACK: First pass sometimes misses data
                retVal.ClassConceptKey = originalClass;
                retVal.DeterminerConceptKey = originalDeterminer;
                retVal.Key = Guid.NewGuid();
                retVal.VersionKey = Guid.NewGuid();
                retVal.LoadProperty(o => o.Relationships).RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
                retVal.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, masterRecord.Key)
                {
                    SourceEntityKey = retVal.Key,
                    ClassificationKey = MdmConstants.SystemClassification
                });
                // Rewrite all master relationships
                //retVal.Relationships.Where(o => o.SourceEntityKey == masterRecord.Key).ToList().ForEach(r => r.SourceEntityKey = retVal.Key);
                return retVal;
            }
            else
            {
                throw new InvalidOperationException(String.Format(ErrorMessages.ARGUMENT_INVALID_TYPE, typeof(Entity), masterRecord.GetType()));
            }
        }

        /// <summary>
        /// Create a new local record for <paramref name="masterRecord"/>
        /// </summary>
        public override TModel CreateLocalFor(TModel masterRecord)
        {
            var retVal = new TModel();
            Guid? originalClass = retVal.ClassConceptKey, originalDeterminer = retVal.DeterminerConceptKey;
            retVal.SemanticCopyNullFields(masterRecord);
            retVal.SemanticCopyNullFields(masterRecord); // HACK: First pass sometimes misses data
            retVal.ClassConceptKey = originalClass;
            retVal.DeterminerConceptKey = originalDeterminer;
            retVal.Key = Guid.NewGuid();
            retVal.VersionKey = Guid.NewGuid();
            retVal.LoadProperty(o => o.Relationships).RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
            retVal.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, masterRecord.Key)
            {
                SourceEntityKey = retVal.Key,
                ClassificationKey = MdmConstants.SystemClassification
            });
            // Rewrite all master relationships
            //retVal.Relationships.Where(o => o.SourceEntityKey == masterRecord.Key).ToList().ForEach(r => r.SourceEntityKey = retVal.Key);
            return retVal;
        }

        /// <summary>
        /// Promote to record of truth
        /// </summary>
        public override TModel PromoteRecordOfTruth(TModel local)
        {
            var master = (Entity)this.GetMasterFor(local);
            if (master == null)
            {
                throw new InvalidOperationException($"Cannot find Master for {local}");
            }
            local.DeterminerConceptKey = MdmConstants.RecordOfTruthDeterminer;

            // Does the local key
            var rotRelationship = local.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship) ??
                master.LoadCollection(o => o.Relationships).SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);

            this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.EditRecordOfTruth);

            if (rotRelationship == null)
            {
                this.m_policyEnforcement.Demand(MdmPermissionPolicyIdentifiers.EstablishRecordOfTruth);

                rotRelationship = new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, local.Key)
                {
                    SourceEntityKey = master.Key,
                    ClassificationKey = MdmConstants.SystemClassification
                };
                local.Relationships.Add(rotRelationship);

                // Ensure the ROT points to the master
                var masterRel = local.Relationships.SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship) ??
                    this.GetMasterRelationshipFor(local.Key.Value) as EntityRelationship;
                if (masterRel.TargetEntityKey != master.Key)
                {
                    local.Relationships.Remove(masterRel);
                    local.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, master.Key)
                    {
                        ClassificationKey = MdmConstants.SystemClassification
                    });
                }
                else
                {
                    local.Relationships.Remove(masterRel);
                    local.Relationships.Add(masterRel);
                }

                // Remove any other MDM relationships
                local.Relationships.RemoveAll(r => r.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship ||
                    r.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship ||
                    r.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship ||
                    r.RelationshipTypeKey == EntityRelationshipTypeKeys.Scoper ||
                    r.RelationshipTypeKey == EntityRelationshipTypeKeys.Replaces
                );

                // Remove all local identifiers for names , addresses, etc.
                local.Names?.ForEach(o =>
                {
                    o.Key = null;
                    ExtensionMethods.ForEach(o.Component, c => c.Key = null);
                    o.SourceEntityKey = null;
                });
                local.Addresses?.ForEach(o =>
                {
                    o.Key = null;
                    o.Component?.ForEach(c => c.Key = null);
                    o.SourceEntityKey = null;
                });
                local.Telecoms?.ForEach(o =>
                {
                    o.Key = null;
                    o.SourceEntityKey = null;
                });
                local.Notes?.Clear();
                local.Participations?.Clear();
                local.Identifiers?.ForEach(o =>
                {
                    o.Key = null;
                    o.SourceEntityKey = null;
                });
                if (local is Person psn)
                {
                    psn.LoadProperty(o => o.LanguageCommunication).ForEach(o =>
                    {
                        o.Key = null;
                        o.SourceEntityKey = null;
                    });
                }
            }
            else if (rotRelationship.SourceEntityKey != master.Key)
            {
                throw new InvalidOperationException("Looks like you're trying to change a ROT relationship to a different master - this is not permitted ");
            }
            else if (local.Key.HasValue && !local.Relationships.Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship))
            {
                var rel = this.GetMasterRelationshipFor(local.Key.Value);
                local.Relationships.Add(rel as EntityRelationship);
            }
            rotRelationship.SourceEntityKey = master.Key;

            // Attribute back to the owner act
            local.CreationActKey = Guid.NewGuid();
            local.CreationAct = new ControlAct()
            {
                Key = local.CreationActKey,
                TypeConceptKey = MdmConstants.MdmControlActType,
                ActTime = DateTimeOffset.Now,
                StatusConceptKey = StatusKeys.Completed,
                MoodConceptKey = ActMoodKeys.Eventoccurrence
            };
            return local;
        }

        /// <summary>
        /// Determine if the entity is already a ROT or wants to be
        /// </summary>
        public override bool IsRecordOfTruth(TModel entity)
        {
            //TODO: Make "T" a constant in MDMConstants.
            if (entity.GetTag(MdmConstants.MdmTypeTag) == "T")
            {
                return true;
            }
            else if (entity.Key.HasValue)
            {
                return this.m_entityPersistenceService.Get(entity.Key.Value, null, AuthenticationContext.SystemPrincipal)?.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer;
            }
            return false;
        }

        /// <summary>
        /// Get master for the specified object
        /// </summary>
        public override IdentifiedData GetMasterFor(TModel local)
        {
            return this.GetMasterFor(local, null);
        }

        /// <summary>
        /// Get master for specified object within a context
        /// </summary>
        private Entity GetMasterFor(TModel local, IEnumerable<IdentifiedData> context)
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

            var masterKey = $"mdm.master.{localKey}";
            EntityRelationship retVal = null; // So older compilers can compile this file
            if (this.m_adhocCache?.TryGet<EntityRelationship>(masterKey, out retVal) == true)
            {
                return retVal;
            }
            else
            {
                retVal = context?.OfType<EntityRelationship>().FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.SourceEntityKey == localKey);
                if (retVal != null)
                {
                    return retVal;
                }
                else
                {
                    retVal = this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.SourceEntityKey == localKey && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                    this.m_adhocCache?.Add(masterKey, retVal);
                    return retVal;
                }
            }
        }

        /// <summary>
        /// Get master relationship for specified local
        /// </summary>
        private EntityRelationship GetMasterRelationshipFor(TModel local, IEnumerable<IdentifiedData> context)
        {
            var masterKey = $"mdm.master.{local.Key}";
            EntityRelationship retVal = null;
            if (this.m_adhocCache?.TryGet<EntityRelationship>(masterKey, out retVal) == true)
            {
                return retVal;
            }
            else
            {
                retVal = local.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship)
                    ?? this.GetMasterRelationshipFor(local.Key, context);
                this.m_adhocCache?.Add(masterKey, retVal);
                return retVal;
            }
        }

        /// <summary>
        /// Extract relationships of note for the MDM layer
        /// </summary>
        public override IEnumerable<ISimpleAssociation> ExtractRelationships(TModel store)
        {
            var retVal = store.LoadProperty(o => o.Relationships)?.Where(o => o.SourceEntityKey.HasValue && o.SourceEntityKey != store.Key).ToList();
            store.Relationships.RemoveAll(o => o.SourceEntityKey.HasValue && o.SourceEntityKey != store.Key);
            return retVal;
        }

        /// <summary>
        /// Refactor relationships
        /// </summary>
        public override void RefactorRelationships(IEnumerable<IdentifiedData> item, Guid fromEntityKey, Guid toEntityKey)
        {

            foreach (var obj in item)
            {
                if (obj is Entity entity)
                {
                    entity.LoadProperty(o => o.Relationships)?.Where(o => o.SourceEntityKey == fromEntityKey && !MdmConstants.MDM_RELATIONSHIP_TYPES.Contains(o.RelationshipTypeKey.GetValueOrDefault())).ToList().ForEach(o => o.SourceEntityKey = toEntityKey);
                    entity.LoadProperty(o => o.Relationships)?.Where(o => o.TargetEntityKey == fromEntityKey && !MdmConstants.MDM_RELATIONSHIP_TYPES.Contains(o.RelationshipTypeKey.GetValueOrDefault())).ToList().ForEach(o => o.TargetEntityKey = toEntityKey);
                }
                else if (obj is Act act)
                {
                    act.LoadProperty(o => o.Participations)?.Where(o => o.PlayerEntityKey == fromEntityKey).ToList().ForEach(o => o.PlayerEntityKey = toEntityKey);
                }
                else if (obj is ITargetedAssociation entityRelationship && !MdmConstants.MDM_RELATIONSHIP_TYPES.Contains(entityRelationship.AssociationTypeKey.GetValueOrDefault()))
                {
                    if (entityRelationship.SourceEntityKey == fromEntityKey)
                    {
                        entityRelationship.SourceEntityKey = toEntityKey;
                    }
                    if (entityRelationship.TargetEntityKey == fromEntityKey)
                    {
                        entityRelationship.TargetEntityKey = toEntityKey;
                    }

                    if (entityRelationship.SourceEntityKey.HasValue)
                    {
                        entityRelationship.SourceEntity = null;
                        entityRelationship.ClearLoaded(o => o.SourceEntity);
                    }
                    if (entityRelationship.TargetEntityKey.HasValue)
                    {
                        entityRelationship.TargetEntity = null;
                        entityRelationship.ClearLoaded(o => o.TargetEntity);
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
                if (data.LoadCollection(o => o.Relationships)?.Count(r => r.RelationshipTypeKey == MdmConstants.MasterRecordClassification && r.ObsoleteVersionSequenceId == null) != 1)
                {
                    yield return new DetectedIssue(DetectedIssuePriorityType.Error, "MDM-ORPHAN", $"{data} appears to be an MDM Orphan", DetectedIssueKeys.FormalConstraintIssue);
                }
            }
        }

        /// <summary>
        /// Perform the specified query against the underlying result set
        /// </summary>
        public override IQueryResultSet<TModel> MdmQuery(NameValueCollection masterQuery, NameValueCollection localQuery, IPrincipal asPrincipal)
        {
            var localLinq = QueryExpressionParser.BuildLinqExpression<Entity>(localQuery, null, false);

            // Try to do a linked query (unless the query is on a special local filter value)
            // TODO: Make it configurable which properties trigger a master query
            var resultSet = this.m_entityPersistenceService.Query(localLinq, AuthenticationContext.SystemPrincipal);
            if (masterQuery.AllKeys.Any())
            {
                var masterLinq = QueryExpressionParser.BuildLinqExpression<Entity>(masterQuery, null, false);
                resultSet = resultSet.Union(masterLinq);
            }

            return new MdmEntityResultSet<TModel>(resultSet.Distinct(), asPrincipal);
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
            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
            {
                var master = this.m_entityPersistenceService.Get(masterKey, null, AuthenticationContext.SystemPrincipal);
                if (master != null)
                {
                    return new EntityMaster<TModel>(master);
                }
                else
                {
                    return null;
                }
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

                if (null != applicationPrincipal)
                {
                    return provenanceApp.Name == applicationPrincipal.Name;
                }
            }

            return provenanceApp.Name == principal.Identity.Name;
        }

        /// <summary>
        /// Establish master for the specified object
        /// </summary>
        public override IdentifiedData EstablishMasterFor(TModel local)
        {
            var retVal = new EntityMaster<TModel>()
            {
                BatchOperation = BatchOperationType.Insert,
                Key = Guid.NewGuid(),
                VersionKey = null,
                CreatedByKey = Guid.Parse(AuthenticationContext.SystemApplicationSid),
                DeterminerConceptKey = DeterminerKeys.Specific,
                TypeConceptKey = local.ClassConceptKey,
                StatusConceptKey = StatusKeys.New
            };
            local.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, local.Key, retVal.Key, MdmConstants.SystemClassification));
            return retVal;
        }

        /// <summary>
        /// Obsolete the specified object
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxObsolete(TModel data, IEnumerable<IdentifiedData> context)
        {
            // First we get the master
            var master = (Entity)this.GetMasterFor(data);

            // How many other relationships are on this master which aren't us and active?
            // No other active?
            if (!this.m_relationshipService.Query(o => o.TargetEntityKey != data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.SourceEntityKey == master.Key && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Any())
            {
                master.BatchOperation = BatchOperationType.Delete;
                yield return master; // ensure master is obsoleted
            }

            data.BatchOperation = BatchOperationType.Delete;

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
                master.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.TargetEntityKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
            if (rotRel == null)
            {
                retVal.AddLast(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, data.Key) { SourceEntityKey = master.Key });
            }
            else if (rotRel.SourceEntityKey != master.Key || rotRel.TargetEntityKey != data.Key) // ROT rel has changed
            {
                rotRel.BatchOperation = BatchOperationType.Delete;
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

            // Remove all relationships from the data and persist at end of bundle
            foreach (var itm in data.Relationships)
            {
                itm.SourceEntityKey = itm.SourceEntityKey ?? data.Key;
                retVal.AddLast(itm);
            }
            data.Relationships.Clear();

            // Attribute back to the owner act
            data.CreationActKey = Guid.NewGuid();
            retVal.AddLast(new ControlAct()
            {
                Key = data.CreationActKey,
                TypeConceptKey = MdmConstants.MdmControlActType,
                ActTime = DateTimeOffset.Now,
                StatusConceptKey = StatusKeys.Completed,
                MoodConceptKey = ActMoodKeys.Eventoccurrence
            });

            return retVal;
        }

        /// <summary>
        /// Save local
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxSaveLocal(TModel data, IEnumerable<IdentifiedData> context)
        {
            // Validate that we can store this in context
            data.RemoveTag(MdmConstants.MdmTypeTag);

            // First, we want to perform a match
            var matchInstructions = this.MdmTxMatchMasters(data, context).ToArray();
            // Persist master in the transaction?
            if (!context.Any(r => r.Key == data.Key))
            {
                // we need to remove any MDM relationships from the object since they'll be in the tx
                data.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship ||
                    o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship ||
                    o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship ||
                    o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship);
                // Add them from the match instructions

                data.Relationships.AddRange(matchInstructions.OfType<EntityRelationship>().Where(o => o.SourceEntityKey == data.Key));

                yield return data;
            }

            foreach (var itm in context)
            {
                yield return itm;
            }

            // Match instructions
            foreach (var rv in matchInstructions)
            {
                yield return rv;
            }

            // Remove all relationships from the data and persist at end of bundle
            foreach (var itm in data.Relationships)
            {
                itm.SourceEntityKey = itm.SourceEntityKey ?? data.Key;
                yield return itm;
            }

            data.Relationships.Clear();

            // Attribute back to the owner act
            if (!data.CreationActKey.HasValue && data.CreationAct == null)
            {
                data.CreationActKey = Guid.NewGuid();
                yield return new ControlAct()
                {
                    Key = data.CreationActKey,
                    TypeConceptKey = MdmConstants.MdmControlActType,
                    ActTime = DateTimeOffset.Now,
                    StatusConceptKey = StatusKeys.Completed,
                    MoodConceptKey = ActMoodKeys.Eventoccurrence
                };
            }

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
            // If the LOCAL is a ROT then don't match them
            if (local.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer)
            {
                yield break;
            }

            // Return value
            LinkedList<IdentifiedData> retVal = new LinkedList<IdentifiedData>();
            bool rematchMaster = false;

            var existingMasterRel = this.GetMasterRelationshipFor(local, context);
            if (existingMasterRel != null)
            {
                if (existingMasterRel.BatchOperation == BatchOperationType.Delete)
                {
                    existingMasterRel = null; // it is being removed
                }
                else
                {
                    if (!existingMasterRel.Key.HasValue)
                    {
                        existingMasterRel = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.TargetEntityKey == existingMasterRel.TargetEntityKey && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, AuthenticationContext.SystemPrincipal).FirstOrDefault() ?? existingMasterRel;
                    }
                    existingMasterRel.BatchOperation = BatchOperationType.InsertOrUpdate;
                    retVal.AddLast(existingMasterRel);
                    rematchMaster = this.m_relationshipService.Query(r => r.TargetEntityKey == existingMasterRel.TargetEntityKey && r.SourceEntityKey != local.Key && r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Any(); // we'll need to rematch

                    // Remove any references to MDM controlled objects in the actual object itself - they'll be used in the TX bundle
                    local.LoadProperty(o => o.Relationships).RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship ||
                        o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship ||
                        o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship);
                }
            }
            var masterKey = $"mdm.master.{local.Key}";
            this.m_adhocCache?.Remove(masterKey);

            // Get the ignore list
            // We ignore any candidate where:
            // TODO: Ensure that this works with the new persistence layer
            // 1. The LOCAL under consideration has an explicit ignore key to a MASTER, or
            var dbIgnoreInstructions = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship, AuthenticationContext.SystemPrincipal);
            // 2. The LOCAL's MASTER is the target of an IGNORE of another local - then those LOCAL MASTERs are ignored or there is an ACTIVE CANDIDIATE
            if (existingMasterRel != null)
            {
                dbIgnoreInstructions = dbIgnoreInstructions.Union(
                        o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship &&
                        o.SourceEntity.Relationships.Where(
                            s => s.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship || s.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship
                        ).Any(
                            s => s.TargetEntityKey == existingMasterRel.TargetEntityKey
                        ));
            }
            var ignoreList = dbIgnoreInstructions.Select(o => o.TargetEntityKey);
            // 3. Ignore all locals of the masters indicated
            ignoreList = ignoreList.Union(this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.ObsoleteVersionSequenceId == null && ignoreList.Contains(o.TargetEntityKey), AuthenticationContext.SystemPrincipal).Select(o => o.SourceEntityKey));

            ignoreList = ignoreList
                .Union(context.OfType<EntityRelationship>().Where(o => o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.BatchOperation != BatchOperationType.Delete).Select(o => o.TargetEntityKey))
                .Union(context.OfType<EntityRelationship>().Where(o => o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.BatchOperation != BatchOperationType.Delete).Select(o => o.SourceEntityKey)).ToArray();

            // It may be possible the ignore was un-ignored
            ignoreList = ignoreList.Where(i => !context.OfType<EntityRelationship>().Any(c => c.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && c.TargetEntityKey == i && c.BatchOperation == BatchOperationType.Delete)).ToList();

            // Existing probable links and set them to obsolete for now
            var existingCandidates = this.m_relationshipService.Query(o => o.SourceEntityKey == local.Key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship, AuthenticationContext.SystemPrincipal);
            foreach (var pl in existingCandidates)
            {
                pl.BatchOperation = BatchOperationType.Delete;
                retVal.AddLast(pl);
            }

            // Match configuration
            // TODO: Emit logs
            var matchResultGrouping = this.m_matchingConfigurationService.Configurations
                .Where(o => o.AppliesTo.Contains(typeof(TModel)) && o.Metadata.Status == MatchConfigurationStatus.Active)
                .SelectMany(s => this.m_matchingService.Match<TModel>(local, s.Id, ignoreList.Select(o => o.Value)))
                .Where(o => o.Record.Key != local.Key) // cannot match with itself
                .Select(o => new MasterMatch(this.IsMaster(o.Record) ? o.Record.Key.Value : this.GetMasterRelationshipFor(o.Record, context).TargetEntityKey.Value, o)) // Must select the master
                .GroupBy(o => o.Master) // We are only interested in the match pairs 1:1 with local and master
                .Select(o => o.OrderByDescending(m => m.MatchResult.Classification).ThenByDescending(m => m.MatchResult.Score).First()) // We are only interested in strongest match pair
                .GroupBy(o => o.MatchResult.Classification) // Group by classifications
                .ToDictionary(o => o.Key, o => o.Distinct()); // Then select by matches


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
            var autoLinkString = String.Empty;
            if (matchResultGrouping[RecordMatchClassification.Match].Count() == 1 &&
                matchResultGrouping[RecordMatchClassification.Match].First().MatchResult.Configuration?.Metadata.Tags.TryGetValue(MdmConstants.AutoLinkSetting, out autoLinkString) == true &&
                Boolean.TryParse(autoLinkString, out var autoLink) && autoLink)
            {
                var matchedMaster = matchResultGrouping[RecordMatchClassification.Match].Single();
                if (existingMasterRel == null) // There is no master, so we can just like create one
                {
                    retVal.AddLast(new EntityRelationship(MdmConstants.MasterRecordRelationship, local.Key, matchedMaster.Master, MdmConstants.AutomagicClassification)
                    {
                        Strength = matchedMaster.MatchResult.Strength,
                        BatchOperation = BatchOperationType.InsertOrUpdate
                    }
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
                            Strength = matchedMaster.MatchResult.Strength,
                            BatchOperation = BatchOperationType.InsertOrUpdate
                        });

                    }
                    else // old master was not verified, so we re-link
                    {
                        var mdmMatchInstructions = this.MdmTxMasterLink(matchedMaster.Master, local.Key.Value, context.Union(retVal), false);

                        foreach (var itm in mdmMatchInstructions)
                        {
                            if (itm is EntityRelationship er && er.ClassificationKey == MdmConstants.SystemClassification) // This is not system it is auto
                            {
                                er.ClassificationKey = MdmConstants.AutomagicClassification;
                            }

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
            else // if (!autoLink || matchResultGrouping[RecordMatchClassification.Match].Count() > 1)
            {
                // Create as candidates for non-existing master
                var nonMasterLinks = matchResultGrouping[RecordMatchClassification.Match].Where(o => o.Master != existingMasterRel?.TargetEntityKey);
                foreach (var nml in nonMasterLinks)
                {
                    retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, nml.Master, MdmConstants.AutomagicClassification) { Strength = nml.MatchResult.Strength, BatchOperation = BatchOperationType.InsertOrUpdate });
                }
            }

            // Candidate matches
            var nonMasterCandidates = matchResultGrouping[RecordMatchClassification.Probable].Where(o => o.Master != existingMasterRel?.TargetEntityKey);
            foreach (var nmc in nonMasterCandidates)
            {
                retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, nmc.Master, MdmConstants.AutomagicClassification) { Strength = nmc.MatchResult.Strength, BatchOperation = BatchOperationType.InsertOrUpdate });

            }

            // Is the existing master rel still in place?
            if (rematchMaster)
            {
                var masterDetail = this.MdmGet(existingMasterRel.TargetEntityKey.Value).Synthesize(AuthenticationContext.SystemPrincipal) as TModel;
                var bestMatch = this.m_matchingConfigurationService.Configurations.Where(o => o.AppliesTo.Contains(typeof(TModel)) && o.Metadata.Status == MatchConfigurationStatus.Active).SelectMany(c => this.m_matchingService.Classify(local, new TModel[] { masterDetail }, c.Id)).OrderByDescending(o => o.Classification).FirstOrDefault();
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
                            foreach (var oldLocal in nonVerifiedLocals)
                            {
                                if (newMaster == null)
                                {
                                    newMaster = (Entity)this.EstablishMasterFor((TModel)oldLocal.LoadProperty(o => o.SourceEntity));
                                    newMaster.BatchOperation = BatchOperationType.InsertOrUpdate;
                                    yield return newMaster;
                                }
                                oldLocal.BatchOperation = BatchOperationType.Delete;
                                yield return oldLocal;
                                yield return new EntityRelationship(MdmConstants.OriginalMasterRelationship, oldLocal.SourceEntityKey, oldLocal.TargetEntityKey, MdmConstants.AutomagicClassification)
                                {
                                    BatchOperation = BatchOperationType.InsertOrUpdate
                                };
                                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, oldLocal.SourceEntityKey, newMaster.Key, MdmConstants.AutomagicClassification)
                                {
                                    BatchOperation = BatchOperationType.InsertOrUpdate
                                };
                            }
                        }
                        else
                        {
                            existingMasterRel.BatchOperation = BatchOperationType.Delete;
                            retVal.AddLast(new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength, BatchOperation = BatchOperationType.InsertOrUpdate });
                            retVal.AddLast(new EntityRelationship(MdmConstants.OriginalMasterRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength, BatchOperation = BatchOperationType.InsertOrUpdate });

                        }
                        break;

                    case RecordMatchClassification.NonMatch:
                        existingMasterRel.BatchOperation = BatchOperationType.Delete;
                        retVal.AddLast(new EntityRelationship(MdmConstants.OriginalMasterRelationship, local.Key, existingMasterRel.TargetEntityKey, MdmConstants.AutomagicClassification) { Strength = bestMatch.Strength });

                        break;

                    case RecordMatchClassification.Match:
                        break;
                }
            }

            // Is there no master link?
            if (!retVal.OfType<EntityRelationship>().Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.ObsoleteVersionSequenceId == null && r.BatchOperation != BatchOperationType.Delete))
            {
                // Return a master at the top of the return list
                yield return this.EstablishMasterFor(local);
                retVal.AddLast(local.Relationships.SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship));
                retVal.Last().BatchOperation = BatchOperationType.InsertOrUpdate;
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
                if (masterRelationships.Any() && masterRelationships.All(o => o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete))
                {
                    yield return masterRelationships.First(); // Return
                    if (originalRelationships.Any()) // There is an original relationship so send that back
                    {
                        yield return originalRelationships.First();
                    }

                    if (candidateRelationships.Any(r => !r.ObsoleteVersionSequenceId.HasValue || r.BatchOperation == BatchOperationType.Delete)) // There is a candidate which is active so send that back
                    {
                        yield return candidateRelationships.FirstOrDefault(o => !o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete);
                    }
                }
                // There is a master to be deleted but not all of them (i.e. there is an active one between L and M)
                // so we just want to keep the current active
                else if (masterRelationships.Any(o => o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete) && masterRelationships.Any(o => !o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation != BatchOperationType.Delete))
                {
                    var masterRel = masterRelationships.First(o => o.ObsoleteVersionSequenceId.HasValue);
                    masterRel.ObsoleteVersionSequenceId = null; // Don't delete it
                    masterRel.BatchOperation = BatchOperationType.Update;
                    masterRel.Strength = masterRelationships.First(o => !o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete).Strength;
                    yield return masterRel;
                }
                else if (masterRelationships.Any(o => !o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation != BatchOperationType.Delete)) // There's a master relationship which is new and not to be deleted
                {
                    yield return masterRelationships.First();
                    if (candidateRelationships.Any(r => r.ObsoleteVersionSequenceId.HasValue || r.BatchOperation == BatchOperationType.Delete)) // There is a candidate
                    {
                        yield return candidateRelationships.FirstOrDefault(o => o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete);
                    }
                }
                // If there is a candidate that is marked as to be deleted
                // but another which is not - we take the candidate with a current
                // key and update the strength (i.e. the candidate still is valid)
                else if (candidateRelationships.Any(r => r.ObsoleteVersionSequenceId.HasValue || r.BatchOperation == BatchOperationType.Delete) && !candidateRelationships.All(r => r.ObsoleteVersionSequenceId.HasValue || r.BatchOperation == BatchOperationType.Delete))
                {
                    var existingRel = candidateRelationships.FirstOrDefault(o => o.ObsoleteVersionSequenceId.HasValue || o.BatchOperation == BatchOperationType.Delete); // the obsoleted one already exists in DB
                    existingRel.Strength = candidateRelationships.First().Strength;
                    existingRel.ObsoleteVersionSequenceId = null;
                    existingRel.BatchOperation = BatchOperationType.Update;
                    yield return existingRel;
                }
                else if (candidateRelationships.Any())
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
            foreach (var itm in retVal)
            {
                if (!(itm is EntityRelationship))
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
                this.m_adhocCache?.Remove($"mdm.master.{localKey}");

                // Obsolete the existing
                if (existingRelationship != null)
                {
                    if (existingRelationship.TargetEntityKey != masterKey)
                    {
                        existingRelationship.BatchOperation = BatchOperationType.Delete;
                        yield return existingRelationship;
                        if (!verified) // store link to original
                        {
                            yield return new EntityRelationship(MdmConstants.OriginalMasterRelationship, localKey, existingRelationship.TargetEntityKey, existingRelationship.ClassificationKey)
                            {
                                BatchOperation = BatchOperationType.InsertOrUpdate
                            };
                        }

                        // Recheck the original master for any other links
                        var dbRels = this.m_relationshipService.Query(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == existingRelationship.TargetEntityKey && r.SourceEntityKey != localKey && r.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Any() ||
                            context.OfType<EntityRelationship>().Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == existingRelationship.TargetEntityKey && r.SourceEntityKey != localKey && r.ObsoleteVersionSequenceId == null);
                        if (!dbRels)
                        {
                            var oldMasterRec = context.OfType<Entity>().FirstOrDefault(o => o.Key == existingRelationship.TargetEntityKey) ??
                                this.GetRaw(existingRelationship.TargetEntityKey.Value) as Entity;

                            // TODO: Add configuration which allows implementers to specify whether they want:
                            //  1. Old masters which have no locals to persist in the database with a replaces relationship (this is useful when a NHID is assigned to the old master and it is REPLACED by a new record)
                            //  2. Old masters which have no locals are removed from the database
                            //oldMasterRec.StatusConceptKey = StatusKeys.Inactive;
                            oldMasterRec.BatchOperation = BatchOperationType.Delete;
                            yield return oldMasterRec;
                            //yield return new EntityRelationship(EntityRelationshipTypeKeys.Replaces, masterKey, oldMasterRec.Key, MdmConstants.SystemClassification)
                            //{
                            //    BatchOperation = BatchOperationType.InsertOrUpdate
                            //};

                        }
                    }
                    else
                    {
                        if (existingRelationship.ClassificationKey == MdmConstants.AutomagicClassification && verified)
                        {
                            existingRelationship.BatchOperation = BatchOperationType.Update;
                            existingRelationship.ClassificationKey = MdmConstants.VerifiedClassification;
                        }
                        yield return existingRelationship;
                        yield break;
                    }
                }

                // Is there an existing candidate link between the two ? If so resolve that
                var existingCandidateRel = this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.SourceEntityKey == localKey && o.TargetEntityKey == masterKey && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                if (existingCandidateRel != null)
                {
                    existingCandidateRel.BatchOperation = BatchOperationType.Delete;
                    yield return existingCandidateRel;
                }

                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, localKey, masterKey, verified ? MdmConstants.VerifiedClassification : existingRelationship.ClassificationKey)
                {
                    BatchOperation = BatchOperationType.InsertOrUpdate
                };
            }
            else
            {
                if (!this.IsMaster(localKey))
                {
                    throw new InvalidOperationException("Cannot link LOCAL to non-MASTER");
                }

                // User had the rel backwards
                foreach (var itm in this.MdmTxMasterLink(localKey, masterKey, context, verified))
                {
                    yield return itm;
                }
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

            if (!this.IsMaster(fromKey) && this.IsMaster(toKey))
            {
                var tkey = fromKey;
                fromKey = toKey;
                toKey = tkey;
            }
            var existingRelationship = this.GetMasterRelationshipFor(toKey, null);
            if (existingRelationship == null || existingRelationship.TargetEntityKey != fromKey)
            {
                throw new InvalidOperationException($"Cannot unlink {toKey} from {fromKey} as MDM relationship in place is between {existingRelationship.SourceEntityKey} and {existingRelationship.TargetEntityKey}");
            }

            // First, add an ignore instruction
            existingRelationship.BatchOperation = BatchOperationType.Delete;

            this.m_adhocCache?.Remove($"mdm.master.{toKey}");

            yield return existingRelationship;

            // Next we we add an ignore
            var ignoreRelationship = new EntityRelationship(MdmConstants.IgnoreCandidateRelationship, existingRelationship.HolderKey, existingRelationship.TargetEntityKey, MdmConstants.VerifiedClassification);
            yield return ignoreRelationship;

            var local = (TModel)existingRelationship.LoadProperty(o => o.SourceEntity);
            // Remove the relationship from the local copy
            local.LoadCollection(o => o.Relationships);
            local.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);

            var localIgnores = this.GetAssociatedLocals(fromKey).Select(o => new EntityRelationship(MdmConstants.IgnoreCandidateRelationship, existingRelationship.HolderKey, o.SourceEntityKey, MdmConstants.SystemClassification));

            // Next, establsh a new MDM master
            foreach (var itm in this.MdmTxMatchMasters(local, new IdentifiedData[] { existingRelationship, ignoreRelationship }.Union(localIgnores)))
            {
                yield return itm;
            }
        }

        /// <summary>
        /// Create transaction instructions to ignore future matches between <paramref name="hostKey"/> and <paramref name="ignoreKey"/>
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, IEnumerable<IdentifiedData> context)
        {
            if (!this.IsMaster(hostKey) && this.IsMaster(ignoreKey))
            {
                var tkey = hostKey;
                hostKey = ignoreKey;
                ignoreKey = tkey;
            }

            // Remove the candidate link
            var candidateLink = this.GetCandidateLocals(hostKey)
                .FirstOrDefault(o => o.SourceEntityKey == ignoreKey) as EntityRelationship;
            if (candidateLink != null)
            {
                this.m_traceSource.TraceUntestedWarning();
                candidateLink.BatchOperation = BatchOperationType.Delete;
                yield return candidateLink;
            }
            yield return new EntityRelationship()
            {
                BatchOperation = BatchOperationType.InsertOrUpdate,
                SourceEntityKey = ignoreKey,
                TargetEntityKey = hostKey,
                ClassificationKey = MdmConstants.VerifiedClassification,
                RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship
            };

            // Add reverse ignores on the master
            // This covers A(LOC)--[IGNORE]-->B(MAS) however if that is true then
            // B(LOC)--[IGNORE]-->A(MAS)
            var existingIgnoreMaster = this.GetMasterRelationshipFor(ignoreKey, context);
            var otherHostLocals = this.GetAssociatedLocals(hostKey);
            // Get all candidate locals for the ignore master
            foreach (var reverseCandidate in this.GetCandidateLocals(existingIgnoreMaster.TargetEntityKey.Value).OfType<EntityRelationship>())
            {
                // Were any of those reverse candidates in the host locals?
                if (otherHostLocals.Any(l => l.SourceEntityKey == reverseCandidate.SourceEntityKey))
                {
                    reverseCandidate.BatchOperation = BatchOperationType.Delete;
                    yield return reverseCandidate; // delete the old candidate reverse relationship
                    yield return new EntityRelationship()
                    {
                        BatchOperation = BatchOperationType.InsertOrUpdate,
                        SourceEntityKey = reverseCandidate.SourceEntityKey,
                        TargetEntityKey = reverseCandidate.TargetEntityKey,
                        ClassificationKey = MdmConstants.VerifiedClassification,
                        RelationshipTypeKey = MdmConstants.IgnoreCandidateRelationship
                    };
                }
            }

        }

        /// <summary>
        /// Gets the local associations (the locals) attached to the master key
        /// </summary>
        public override IQueryResultSet<ITargetedAssociation> GetAssociatedLocals(Guid masterKey)
        {
            return this.m_relationshipService
                .Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Gets the candidate locals of a specified master
        /// </summary>
        /// <param name="masterKey">The master key to fetch</param>
        /// <returns>The candidate locals which have not been established</returns>
        public override IQueryResultSet<ITargetedAssociation> GetCandidateLocals(Guid masterKey)
        {
            return this.m_relationshipService
                .Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Get all candidates which should be ignored
        /// </summary>
        public override IQueryResultSet<ITargetedAssociation> GetIgnoredCandidateLocals(Guid masterKey)
        {
            return this.m_relationshipService
                .Query(o => o.TargetEntityKey == masterKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Get all candidate locals
        /// </summary>
        public override IQueryResultSet<ITargetedAssociation> GetAllMdmCandidateLocals()
        {
            return this.m_relationshipService
                .Query(o => o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Get the master construct record for <paramref name="masterKey"/>
        /// </summary>
        public override IMdmMaster GetMasterFor(Guid masterKey)
        {
            var masterEntity = this.m_entityPersistenceService.Get(masterKey, null, AuthenticationContext.SystemPrincipal) as Entity;
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
        public override IQueryResultSet<ITargetedAssociation> GetEstablishedCandidateMasters(Guid localKey)
        {
            return this.m_relationshipService
                .Query(o => o.SourceEntityKey == localKey && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Get all ignored masters
        /// </summary>
        public override IQueryResultSet<ITargetedAssociation> GetIgnoredMasters(Guid localKey)
        {
            return this.m_relationshipService
                .Query(o => o.SourceEntityKey == localKey && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal)
                .TransformResultSet<EntityRelationship, ITargetedAssociation>();
        }

        /// <summary>
        /// Create transaction instructiosn to merge <paramref name="victimKey"/> into <paramref name="survivorKey"/>
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxMergeMasters(Guid survivorKey, Guid victimKey, IEnumerable<IdentifiedData> context)
        {
            // First enusre validate state
            if (!this.IsMaster(survivorKey) || !this.IsMaster(victimKey))
            {
                throw new InvalidOperationException($"Both {survivorKey} and {victimKey} must be MASTER");
            }

            // First we obsolete the old
            var survivorData = this.GetRaw(survivorKey) as Entity;
            var victimData = this.GetRaw(victimKey) as Entity;
            victimData.StatusConceptKey = StatusKeys.Obsolete; // The MERGE indicates the OLD data is OBSOLETE (i.e. no longer accurate)
            yield return victimData;

            // Associated locals for the victim are mapped
            foreach (var rel in this.GetAssociatedLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.BatchOperation = BatchOperationType.Delete;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.MasterRecordRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey)
                {
                    Strength = rel.Strength
                };
            }

            // Associated candidates are mapped
            foreach (var rel in this.GetIgnoredCandidateLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.BatchOperation = BatchOperationType.Delete;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.IgnoreCandidateRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey);
            }

            // Associated matches are mapped
            foreach (var rel in this.GetCandidateLocals(victimKey).OfType<EntityRelationship>())
            {
                rel.BatchOperation = BatchOperationType.Delete;
                yield return rel;
                yield return new EntityRelationship(MdmConstants.CandidateLocalRelationship, rel.SourceEntityKey, survivorKey, rel.ClassificationKey);
            }

            // Identifiers for the victim are obsoleted and migrated
            foreach (var ident in victimData.LoadCollection(o => o.Identifiers).Where(o => !survivorData.LoadCollection(s => s.Identifiers).Any(s => !s.SemanticEquals(o))))
            {
                ident.BatchOperation = BatchOperationType.Delete;
                yield return ident;
                yield return new EntityIdentifier(ident.IdentityDomain, ident.Value)
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
            return
                this.m_relationshipService.Query(
                    o => (o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship) && o.ObsoleteVersionSequenceId == null && o.SourceEntityKey == localKey, AuthenticationContext.SystemPrincipal)
                    .Union(this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null && o.TargetEntityKey == localKey, AuthenticationContext.SystemPrincipal))
                    .OfType<ITargetedAssociation>();
        }

        /// <summary>
        /// Given a master <paramref name="master"/> - detect LOCALs which could be candidates
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxDetectCandidates(IdentifiedData master, List<IdentifiedData> context)
        {
            if (!this.IsMaster(master.Key.Value))
            {
                throw new ArgumentException("MdmTxDetectCandidiates expects MASTER record");
            }

            // Get the ignore list
            var ignoreList = this.m_relationshipService.Query(o => o.TargetEntityKey == master.Key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship, AuthenticationContext.SystemPrincipal).Select(o => o.SourceEntityKey.Value);//.Select(o => this.GetMasterRelationshipFor(o.SourceEntityKey.Value, context).TargetEntityKey.Value);

            // iterate through configuration
            foreach (var config in this.m_matchingConfigurationService.Configurations.Where(o => o.AppliesTo.Contains(typeof(TModel)) && o.Metadata.Status == MatchConfigurationStatus.Active))
            {
                // perform match
                var results = this.m_matchingService.Match(master, config.Id, ignoreList);

                // Return the results which are not the master
                foreach (var r in results.Where(r => r.Classification != RecordMatchClassification.NonMatch && r.Record.Key != master.Key))
                {
                    // We have a master - so we want to get the locals for checking / insert
                    if (this.IsMaster(r.Record.Key.Value))
                    {
                        foreach (var local in this.GetAssociatedLocals(r.Record.Key.Value).ToList().Where(q => !ignoreList.Contains(q.SourceEntityKey.Value)))
                        {
                            var src = local.LoadProperty(o => o.SourceEntity) as Entity;
                            if (src.DeterminerConceptKey != MdmConstants.RecordOfTruthDeterminer)
                            {
                                yield return new EntityRelationship(MdmConstants.CandidateLocalRelationship, local.SourceEntityKey, master.Key, MdmConstants.AutomagicClassification)
                                {
                                    Strength = r.Strength
                                };
                            }
                        }
                    }
                    else if (r.Record is TModel tm && tm.DeterminerConceptKey != MdmConstants.RecordOfTruthDeterminer &&
                        this.GetMasterRelationshipFor(r.Record.Key.Value).TargetEntityKey != master.Key)
                    {
                        yield return new EntityRelationship(MdmConstants.CandidateLocalRelationship, r.Record.Key.Value, master.Key, MdmConstants.AutomagicClassification)
                        {
                            Strength = r.Strength
                        };
                    }
                }
            }
        }

        /// <summary>
        /// Un-ignore an ignore link
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxUnIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, List<IdentifiedData> context)
        {
            // First
            if (this.IsMaster(hostKey))
            {
                // Locate the current ignore key
                var existingIgnoreKey = this.GetIgnoredCandidateLocals(hostKey)
                    .FirstOrDefault(t => t.SourceEntityKey == ignoreKey) as EntityRelationship;
                if (existingIgnoreKey == null)
                {
                    yield break; // no ignored anyways
                }

                // We delete the ignore
                existingIgnoreKey.BatchOperation = BatchOperationType.Delete;
                yield return existingIgnoreKey;

                // Next - we want to re-match
                foreach (var itm in this.MdmTxMatchMasters(existingIgnoreKey.LoadProperty(o => o.SourceEntity), context))
                {
                    yield return itm;
                }
            }
            else if (this.IsMaster(ignoreKey)) // reversed
            {
                foreach (var itm in this.MdmTxUnIgnoreCandidateMatch(ignoreKey, hostKey, context))
                {
                    yield return itm;
                }
            }
        }

        /// <summary>
        /// Create master container for the specified object
        /// </summary>
        public override IMdmMaster CreateMasterContainerForMasterEntity(IAnnotatedResource masterObject) => new EntityMaster<TModel>(masterObject as Entity);

        /// <summary>
        /// Determine if the record is a local
        /// </summary>
        public override bool IsLocal(Guid dataKey) => this.m_relationshipService.Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && o.ObsoleteVersionSequenceId == null && o.SourceEntityKey == dataKey, AuthenticationContext.SystemPrincipal).Any();

        /// <summary>
        /// Determine if the local is owned by the principal
        /// </summary>
        public override bool IsOwner(Guid localKey, IPrincipal principal)
        {
            var identity = principal.Identity;
            if (principal is IClaimsPrincipal claimsPrincipal)
            {
                identity = claimsPrincipal?.Identities.OfType<IDeviceIdentity>().FirstOrDefault() as IIdentity ??
                            claimsPrincipal?.Identities.OfType<IApplicationIdentity>().FirstOrDefault();
            }

            // Identity
            if (identity is IDeviceIdentity deviceIdentity)
            {
                return this.m_persistenceService.Query(o => o.Key == localKey && o.CreatedBy.Device.Name == deviceIdentity.Name, AuthenticationContext.SystemPrincipal).Any();
            }
            else if (identity is IApplicationIdentity applicationIdentity)
            {
                return this.m_persistenceService.Query(o => o.Key == localKey && o.CreatedBy.Application.Name == applicationIdentity.Name, AuthenticationContext.SystemPrincipal).Any();
            }
            else
            {
                throw new InvalidOperationException("MDM only works on Device and Application identities");
            }
        }

        
    }
}
