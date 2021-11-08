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
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Exceptions;
using SanteDB.Core.Jobs;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Patch;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Model.Security;
using SanteDB.Core.Model.Serialization;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Security.Principal;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Jobs;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Principal;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Abstract wrapper for MDM resource listeners
    /// </summary>
    ///
    public abstract class MdmResourceListener : IDisposable
    {
        /// <summary>
        /// Dispose
        /// </summary>
        public abstract void Dispose();
    }

    /// <summary>
    /// Represents a base class for an MDM resource listener
    /// </summary>
    public class MdmResourceListener<T> : MdmResourceListener, IRecordMergingService<T>
        where T : IdentifiedData, new()
    {
        /// <summary>
        ///  Status which be merged
        /// </summary>
        private readonly Guid[] m_mergeStates = new Guid[]
        {
            StatusKeys.Active,
            StatusKeys.Cancelled,
            StatusKeys.Completed,
            StatusKeys.New
        };

        // Match comparison
        private static MasterMatchEqualityComparer s_matchComparer = new MasterMatchEqualityComparer();

        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => $"MDM Data Handler Listener for {typeof(T).FullName}";

        // Configuration
        private ResourceMergeConfiguration m_resourceConfiguration;

        // Tracer
        private readonly Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // The repository that this listener is attached to
        private INotifyRepositoryService<T> m_repository;

        // Persistence service
        private IDataPersistenceService<Bundle> m_bundlePersistence;

        // Persistence service
        private IDataPersistenceService<T> m_dataPersistenceService;

        // Raw master persistence service
        private IDataPersistenceService m_rawMasterPersistenceService;

        // Unique authorities
        private IEnumerable<AssigningAuthority> m_uniqueAuthorities;

        /// <summary>
        /// Match job
        /// </summary>
        private MdmMatchJob<T> m_backgroundMatch;

        /// <summary>
        /// Fired when the service is merging
        /// </summary>
        public event EventHandler<DataMergingEventArgs<T>> Merging;

        /// <summary>
        /// Fired when data has been merged
        /// </summary>
        public event EventHandler<DataMergeEventArgs<T>> Merged;

        /// <summary>
        /// Fired when data is un-merging
        /// </summary>
        public event EventHandler<DataMergingEventArgs<T>> UnMerging;

        /// <summary>
        /// Fired when data has been un-merged
        /// </summary>
        public event EventHandler<DataMergeEventArgs<T>> UnMerged;

        /// <summary>
        /// Resource listener
        /// </summary>
        public MdmResourceListener(ResourceMergeConfiguration configuration)
        {
            // Register the master

            ModelSerializationBinder.RegisterModelType($"{typeof(T).Name}Master", typeof(Entity).IsAssignableFrom(typeof(T)) ? typeof(EntityMaster<T>) : typeof(ActMaster<T>));
            this.m_resourceConfiguration = configuration;
            this.m_bundlePersistence = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Bundle>>();
            if (this.m_bundlePersistence == null)
                throw new InvalidOperationException($"Could not find persistence service for Bundle");
            this.m_dataPersistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>();

            var matchConfigService = ApplicationServiceContext.Current.GetService<IRecordMatchingConfigurationService>();
            foreach (var itm in configuration.MatchConfiguration)
                if (matchConfigService.GetConfiguration(itm) == null)
                    throw new InvalidOperationException($"Can't find match configuration {itm}");

            if (typeof(Act).IsAssignableFrom(typeof(T)))
            {
                ModelSerializationBinder.RegisterModelType(typeof(ActMaster<T>));
                this.m_rawMasterPersistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Act>>() as IDataPersistenceService;
            }
            else
            {
                ModelSerializationBinder.RegisterModelType(typeof(EntityMaster<T>));
                this.m_rawMasterPersistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>() as IDataPersistenceService;
            }

            this.m_backgroundMatch = new MdmMatchJob<T>();
            ApplicationServiceContext.Current.GetService<IJobManagerService>()?.AddJob(this.m_backgroundMatch, TimeSpan.MaxValue, JobStartType.Never);

            this.m_repository = ApplicationServiceContext.Current.GetService<IRepositoryService<T>>() as INotifyRepositoryService<T>;
            if (this.m_repository == null)
                throw new InvalidOperationException($"Could not find repository service for {typeof(T)}");

            // Subscribe
            this.m_repository.Inserting += this.OnPrePersistenceValidate;
            this.m_repository.Saving += this.OnPrePersistenceValidate;
            this.m_repository.Obsoleting += this.OnPrePersistenceValidate;
            this.m_repository.Inserting += this.OnInserting;
            this.m_repository.Saving += this.OnSaving;
            this.m_repository.Obsoleting += this.OnObsoleting;
            this.m_repository.Retrieved += this.OnRetrieved;
            this.m_repository.Retrieving += this.OnRetrieving;
            this.m_repository.Queried += this.OnQueried;
            this.m_repository.Querying += this.OnQuerying;
        }

        /// <summary>
        /// Handles before a subscribe object is queried
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks><para>Unless a local tag is specified, this command will ensure that only MASTER records are returned
        /// to the client, otherwise it will ensure only LOCAL records are returned.</para>
        /// <para>This behavior ensures that clients interested in LOCAL records only get only their locally contributed records
        /// otherwise they will receive the master records.
        /// </para>
        /// </remarks>
        protected virtual void OnQuerying(object sender, QueryRequestEventArgs<T> e)
        {
            var query = new NameValueCollection(QueryExpressionBuilder.BuildQuery<T>(e.Query).ToArray());

            // The query doesn't contain a query for master records, so...
            // If the user is not in the role "SYSTEM" OR they didn't ask specifically for LOCAL records we have to rewrite the query to use MASTER
            if (!e.Principal.IsInRole("SYSTEM") || !query.ContainsKey("tag[mdm.type].value"))
            {
                // Did the person ask specifically for a local record? if so we need to demand permission
                if (query.TryGetValue("tag[mdm.type].value", out List<String> mdmFilters) && mdmFilters.Contains("L"))
                    ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);
                else // We want to modify the query to only include masters and rewrite the query
                {
                    var localQuery = new NameValueCollection(query.ToDictionary(o => $"relationship[{MdmConstants.MasterRecordRelationship}].source@{typeof(T).Name}.{o.Key}", o => o.Value));
                    query.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
                    //localQuery.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
                    e.Cancel = true; // We want to cancel the other's query

                    // We are wrapping an entity, so we query entity masters
                    int tr = 0;
                    if (typeof(Entity).IsAssignableFrom(typeof(T)))
                        e.Results = this.MasterQuery<Entity>(query, localQuery, e.QueryId.GetValueOrDefault(), e.Offset, e.Count, e.Principal, out tr);
                    else
                        e.Results = this.MasterQuery<Act>(query, localQuery, e.QueryId.GetValueOrDefault(), e.Offset, e.Count, e.Principal, out tr);
                    e.TotalResults = tr;
                }
            }
        }

        /// <summary>
        /// Perform a master query
        /// </summary>
        /// <param name="count">The number of results to return</param>
        /// <param name="localQuery">The query for local records affixed to the MDM tree</param>
        /// <param name="masterQuery">The query for master records</param>
        /// <param name="offset">The offset of the first result</param>
        /// <param name="principal">The user executing the query</param>
        /// <param name="queryId">The unique query identifier</param>
        ///<param name="totalResults">The number of matching results</param>
        internal IEnumerable<T> MasterQuery<TMasterType>(NameValueCollection masterQuery, NameValueCollection localQuery, Guid queryId, int offset, int? count, IPrincipal principal, out int totalResults)
            where TMasterType : IdentifiedData
        {
            var qpi = ApplicationServiceContext.Current.GetService<IStoredQueryDataPersistenceService<TMasterType>>();
            IEnumerable<TMasterType> results = null;
            if (qpi is IUnionQueryDataPersistenceService<TMasterType> iqps)
            {
                // Try to do a linked query (unless the query is on a special local filter value)
                try
                {
                    var localLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(localQuery, null, false);

                    // Only identifiers can be appended to the master query
                    // TODO: Make this separable so that we can control master properties
                    if (masterQuery.Keys.Any(o => o.StartsWith("identifier")))
                    {
                        var masterLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(masterQuery, null, false);
                        results = iqps.Union(new Expression<Func<TMasterType, bool>>[] { localLinq, masterLinq }, queryId, offset, count, out totalResults, principal);
                    }
                    else
                    {
                        results = qpi.Query(localLinq, queryId, offset, count, out totalResults, principal);
                    }
                }
                catch
                {
                    var localLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(localQuery, null, false);
                    results = qpi.Query(localLinq, queryId, offset, count, out totalResults, principal);
                }
            }
            else
            { // Not capable of doing intersect results at query level
                var masterLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(localQuery, null, false);
                results = qpi.Query(masterLinq, queryId, offset, count, out totalResults, principal);
            }
            return results.AsParallel().AsOrdered().Select(o => o is Entity ? new EntityMaster<T>((Entity)(object)o).GetMaster(principal) : new ActMaster<T>((Act)(Object)o).GetMaster(principal)).OfType<T>().ToList();
        }

        /// <summary>
        /// Handles when a subscribed object is queried
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>The MDM provider will ensure that no data from the LOCAL instance which is masked is returned
        /// in the MASTER record</remarks>
        protected virtual void OnQueried(object sender, QueryResultEventArgs<T> e)
        {
        }

        /// <summary>
        /// Handles when subscribed object is being retrieved
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>The MDM records are actually redirected types. For example, a request to retrieve a Patient
        /// which is a master is actually retrieving an entity which has a synthetic record of type Patient. If we
        /// don't redirect these requests then technically a request to retrieve a master will result in an emtpy / exception
        /// case.</remarks>
        protected virtual void OnRetrieving(object sender, DataRetrievingEventArgs<T> e)
        {
            // There aren't actually any data in the database which is of this type
            // This is to prevent the MDM layer from hijacking a history request for a particular version of the object
            ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>().Query(o => o.Key == e.Id, 0, 0, out int records, AuthenticationContext.SystemPrincipal);
            if (records == 0) //
            {
                e.Cancel = true;
                if (typeof(Entity).IsAssignableFrom(typeof(T)))
                {
                    var master = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>().Get(e.Id.Value, null, AuthenticationContext.Current.Principal);
                    e.Result = new EntityMaster<T>(master).GetMaster(AuthenticationContext.Current.Principal);
                }
                else if (typeof(Act).IsAssignableFrom(typeof(T)))
                {
                    var master = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Act>>().Get(e.Id.Value, null, AuthenticationContext.Current.Principal);
                    e.Result = new ActMaster<T>(master).GetMaster(AuthenticationContext.Current.Principal);
                }
            }
        }

        /// <summary>
        /// Handles when subscribed object is retrieved
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>MDM provider will ensure that if the retrieved record is a MASTER record, that no
        /// data from masked LOCAL records is included.</remarks>
        protected virtual void OnRetrieved(object sender, DataRetrievedEventArgs<T> e)
        {
            // We have retrieved an object from the database. If it is local we have to ensure that
            // 1. The user actually requested the local
            // 2. The user is the original owner of the local, or
            // 2a. The user has READ LOCAL permission
            if ((e.Data as ITaggable)?.Tags.Any(t => t.TagKey == "$mdm.type" && t.Value == "L") == true) // is a local record
            {
                // Is the requesting user the provenance of that record?
                this.EnsureProvenance(e.Data as BaseEntityData, AuthenticationContext.Current.Principal);
            }
        }

        /// <summary>
        /// Validates that a MASTER record is only being inserted by this class
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>We don't want clients submitting MASTER records, so this method will ensure that all records
        /// being sent with tag of MASTER are indeed sent by the MDM or SYSTEM user.</remarks>
        protected virtual void OnPrePersistenceValidate(object sender, DataPersistingEventArgs<T> e)
        {
            // Only the server is allowed to establish master records , clients are not permitted
            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client ||
                ApplicationServiceContext.Current.HostType == SanteDBHostType.Gateway)
                return;

            var identified = e.Data as IdentifiedData;
            var idpType = typeof(IDataPersistenceService<>).MakeGenericType(e.Data is Entity ? typeof(Entity) : typeof(Act));
            var idp = ApplicationServiceContext.Current.GetService(idpType) as IDataPersistenceService;
            var mdmTag = (e.Data as ITaggable).Tags.FirstOrDefault(o => o.TagKey == "$mdm.type");

            // Maybe a current value? Let's check the database and
            // open the existing, this may be a master record...
            if (mdmTag?.Value != "M" && e.Data.Key.HasValue || mdmTag?.Value == "M")
            {
                var existing = idp.Get(e.Data.Key.GetValueOrDefault()) as IIdentifiedEntity;
                Guid? classConcept = (existing as Entity)?.ClassConceptKey ?? (existing as Act)?.ClassConceptKey;

                // This entity is attempting to update a master record, however the incoming data is not an appropriate type for MDM management
                // They must intend to submit the data as a new local
                if ((classConcept.GetValueOrDefault() == MdmConstants.MasterRecordClassification) && !e.Principal.IsInRole("SYSTEM") &&
                    existing.GetType() != e.Data.GetType())
                {
                    // Entity being persisted is an entity (data needs to migrated to either the local which belongs to the application or a new local if an existing local doesn't exist)
                    if ((object)e.Data is Entity dataEntity)
                    {
                        var existingLocal = this.GetLocalFor(existing, AuthenticationContext.Current.Principal as IClaimsPrincipal) as Entity;
                        // We're updating the existing local identity
                        dataEntity.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
                        if (existingLocal != null)
                        {
                            dataEntity.VersionKey = null;
                            dataEntity.Key = existingLocal.Key;
                            dataEntity.Tags.RemoveAll(o => o.TagKey == "$mdm.type" || o.TagKey.StartsWith("$"));
                            existingLocal.CopyObjectData(dataEntity, true, true);
                            existingLocal.CopyObjectData(dataEntity, true, true); // HACK: Run again to clear out any delayed load properties
                            identified = e.Data = (T)(object)existingLocal;
                        }
                        else // We're creating a new local entity for this system
                        {
                            e.Data.Key = identified.Key = Guid.NewGuid(); // New key
                            dataEntity.VersionKey = Guid.NewGuid();
                            dataEntity.VersionSequence = null;
                            dataEntity.Tags.RemoveAll(o => o.TagKey == "$mdm.type" || o.TagKey.StartsWith("$"));

                            if (mdmTag?.Value == "T") // They want this record to be a record of truth
                            {
                                // The application identity or device identity must have MDM master unlimited
                                ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.UnrestrictedMdm);

                                dataEntity.DeterminerConceptKey = MdmConstants.RecordOfTruthDeterminer;
                                dataEntity.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, existing as Entity)
                                {
                                    ClassificationKey = MdmConstants.VerifiedClassification
                                });
                                dataEntity.Tags.Add(new EntityTag("$mdm.type", "T"));
                            }
                            else // it is just another record
                            {
                                dataEntity.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, existing as Entity)
                                {
                                    ClassificationKey = MdmConstants.VerifiedClassification
                                });
                            }
                        }

                        // Rewrite any relationships ...
                        dataEntity.Relationships.ForEach(o => this.RefactorRelationship(o, existing.Key.Value, dataEntity.Key.Value));
                        if (sender is Bundle bundleS && e.Data.Key != existing.Key)
                        {
                            foreach (var er in bundleS.Item.OfType<ITargetedAssociation>().Where(o => o.SourceEntityKey == existing.Key || o.TargetEntityKey == existing.Key).ToArray())
                            {
                                this.RefactorRelationship(er, existing.Key.Value, dataEntity.Key.Value);
                            }
                        }

                        dataEntity.StripAssociatedItemSources();
                    }
                }
                // Entity is not a master so don't do anything
            }

            // Data being persisted is an entity
            if (e.Data is Entity entityData)
            {
                // Correct relationship and determine if re-association is being assigned
                var eRelationship = entityData.GetRelationships().FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship);
                if (eRelationship != null)
                {
                    // Get existing er if available
                    var dbRelationship = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.RelationshipTypeKey == eRelationship.RelationshipTypeKey && o.SourceEntityKey == identified.Key, 0, 1, out int tr, e.Principal);
                    if (tr == 0 || dbRelationship.First().TargetEntityKey == eRelationship.TargetEntityKey)
                        return;
                    else if (!e.Principal.IsInRole("SYSTEM")) // The target entity is being re-associated make sure the principal is allowed to do this
                        ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
                }

                if (entityData?.Tags?.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true &&
                        entityData.Relationships?.Single(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship) == null)
                    throw new InvalidOperationException("Records of truth must have exactly one MASTER");
            }
            else if (e.Data is Act)
            {
                var eRelationship = (e.Data as Act).LoadCollection<ActRelationship>("Relationships").FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship);
                if (eRelationship != null)
                {
                    // Get existing er if available
                    var dbRelationship = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.SourceEntityKey == identified.Key, 0, 1, out int tr, e.Principal);
                    if (tr == 0 || dbRelationship.First().TargetActKey == eRelationship.TargetActKey)
                        return;
                    else if (!e.Principal.IsInRole("SYSTEM")) // The target entity is being re-associated make sure the principal is allowed to do this
                        ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
                }

                if ((e.Data as ITaggable)?.Tags.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true &&
                    (e.Data as Act).Relationships.Single(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship) == null)
                    throw new InvalidOperationException("Records of truth must have exactly one MASTER");
            }
        }

        /// <summary>
        /// Refactor the relationship <paramref name="relationship"/> from <paramref name="fromKey"/> to <paramref name="toKey"/>
        /// </summary>
        private void RefactorRelationship(ITargetedAssociation relationship, Guid fromKey, Guid toKey)
        {
            if (relationship.SourceEntityKey == fromKey) { relationship.SourceEntityKey = toKey; }
            else if (relationship.TargetEntityKey == fromKey) { relationship.TargetEntityKey = toKey; }
        }

        /// <summary>
        /// Get the registered local for the specified master
        /// </summary>
        private IIdentifiedEntity GetLocalFor(IIdentifiedEntity data, IClaimsPrincipal principal)
        {
            IIdentity identity = principal?.Identities.OfType<IDeviceIdentity>().FirstOrDefault() as IIdentity ??
                            principal?.Identities.OfType<IApplicationIdentity>().FirstOrDefault() as IIdentity;

            if (data is Entity dataEntity)
            {
                var idp = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>();

                if (dataEntity.Tags.FirstOrDefault(o => o.TagKey == "$mdm.type")?.Value == "T" || dataEntity.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer) // Record of truth we must look for
                    return idp.Query(o => o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-RecordOfTruth").Any(g => g.SourceEntityKey == dataEntity.Key), 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                else if (identity is IDeviceIdentity deviceIdentity)
                    return idp.Query(o => o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-Master").Any(g => g.TargetEntityKey == dataEntity.Key) && o.CreatedBy.Device.Name == deviceIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                else if (identity is IApplicationIdentity applicationIdentity)
                    return idp.Query(o => o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-Master").Any(g => g.TargetEntityKey == dataEntity.Key) && o.CreatedBy.Application.Name == applicationIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                else
                    return null;
            }
            else
                throw new InvalidOperationException("Unsupported resource type");
        }

        /// <summary>
        /// Fired before a record is obsoleted
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>We don't want a MASTER record to be obsoleted under any condition. MASTER records require special permission to
        /// obsolete and also require that all LOCAL records be either re-assigned or obsoleted as well.</remarks>
        protected virtual void OnObsoleting(object sender, DataPersistingEventArgs<T> e)
        {
            // Only the server is allowed to establish master records , clients are not permitted
            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client ||
                ApplicationServiceContext.Current.HostType == SanteDBHostType.Gateway)
                return;

            // Obsoleting a master record requires that the user be a SYSTEM user or has WriteMDM permission
            Guid? classConcept = (e.Data as Entity)?.ClassConceptKey ?? (e.Data as Act)?.ClassConceptKey;
            // We are touching a master record and we are not system?
            if (classConcept.GetValueOrDefault() == MdmConstants.MasterRecordClassification &&
                !e.Principal.IsInRole("SYSTEM"))
                ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);

            // Typically by the time that this method is called, the pre persistence trigger has already replaced the e.Data reference to the
            // appropriate local record. We should, however, ensure that the MASTER record is appropriately not orphaned
            var master = this.GetMaster(e.Data);
            if (e.Data is Entity entityData)
            {
                var otherLocals = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.TargetEntityKey == master && o.SourceEntityKey != e.Data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, AuthenticationContext.SystemPrincipal).Any();
                if (!otherLocals) // There are no other locals
                {
                    var insertData = new Bundle()
                    {
                        Item =
                        {
                            e.Data,
                            new Entity()
                            {
                                Key = master
                            }
                        }
                    };
                    this.m_bundlePersistence.Obsolete(insertData, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
            }
            else if (e.Data is Act actData)
            {
                var otherLocals = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.TargetActKey == master && o.SourceEntityKey != e.Data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, AuthenticationContext.SystemPrincipal).Any();
                if (!otherLocals) // There are no other locals
                {
                    var insertData = new Bundle()
                    {
                        Item =
                        {
                            e.Data,
                            new Act()
                            {
                                Key = master
                            }
                        }
                    };
                    this.m_bundlePersistence.Obsolete(insertData, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
            }
            else
                throw new InvalidOperationException($"Cannot apply MDM rules to {e.Data.GetType().Name}");
        }

        /// <summary>
        /// Called when a record is being created or updated
        /// </summary>
        protected virtual void OnSaving(object sender, DataPersistingEventArgs<T> e)
        {
            // Only the server is allowed to establish master records , clients are not permitted
            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client ||
                ApplicationServiceContext.Current.HostType == SanteDBHostType.Gateway)
                return;

            // Already processed
            if (e.Data is ITaggable taggable)
            {
                if (taggable.Tags.Any(o => o.TagKey == "$mdm.processed"))
                    return;
                else
                    taggable.AddTag("$mdm.processed", "true");
            }
            try
            {
                // Is this object a ROT or MASTER, if it is then we do not perform any changes to re-binding
                if (this.IsRecordOfTruth(e.Data))
                {
                    // Record of truth, ensure we update the appropirate
                    if (e.Data is Entity entityData)
                    {
                        // Establish a master
                        var bundle = sender as Bundle ?? new Bundle();
                        if (!bundle.Item.Contains(e.Data))
                            bundle.Add(e.Data);
                        bundle.Item.InsertRange(0, this.RefactorMdmTargetsToLocals(e.Data, bundle));

                        // Get the MDM relationship as this record will point > MDM
                        var masterRelationship = entityData.Relationships.FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                        if (masterRelationship == null) // Attempt to load from DB
                        {
                            masterRelationship = this.GetRelationshipTargets(e.Data, MdmConstants.MasterRecordRelationship).FirstOrDefault() as EntityRelationship;
                            if (masterRelationship == null) // Oddly this ROT doesn't have a master, we should restore it
                            {
                                this.m_traceSource.TraceWarning("ROT does not belong to a master, adding link based on ROT relationship");
                                var rotRelationship = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.TargetEntityKey == e.Data.Key && o.ObsoleteVersionSequenceId == null && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship, 0, 2, out int _, AuthenticationContext.SystemPrincipal).SingleOrDefault();
                                if (rotRelationship == null)
                                    throw new MdmException(e.Data, $"Unable to determine master record for {e.Data}");
                                else
                                {
                                    masterRelationship = new EntityRelationship(MdmConstants.MasterRecordRelationship, rotRelationship.SourceEntityKey) { SourceEntityKey = rotRelationship.TargetEntityKey, ClassificationKey = MdmConstants.VerifiedClassification };
                                    bundle.Add(masterRelationship);
                                }
                            }
                        }

                        // Is there already a master of truth relationship for a different record for the master we're pointing at?
                        var existingRotRel = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.SourceEntityKey == masterRelationship.TargetEntityKey && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (existingRotRel != null)
                        {
                            // is it pointing at a different record?
                            if (existingRotRel.TargetEntityKey != entityData.Key)
                            {
                                this.m_traceSource.TraceUntestedWarning();
                                // Remove the old
                                existingRotRel.ObsoleteVersionSequenceId = Int32.MaxValue;
                                bundle.Add(existingRotRel);
                                // Add the new
                                bundle.Add(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, entityData.Key) { SourceEntityKey = masterRelationship.TargetEntityKey, ClassificationKey = MdmConstants.VerifiedClassification });
                            }
                        }
                        else
                            bundle.Add(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, entityData.Key) { SourceEntityKey = masterRelationship.TargetEntityKey, ClassificationKey = MdmConstants.VerifiedClassification });

                        // Is there already a master of truth relationship for this object which is a different master than the one specified?
                        if (ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Count(o => o.TargetEntityKey == e.Data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.SourceEntityKey != masterRelationship.TargetEntityKey && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal) > 0)
                        {
                            throw new InvalidOperationException("You're trying to set this record as the record of truth for two different masters. This is not allowed");
                        }

                        // Sender is not bundle so this isn't a chained process
                        if (!(sender is Bundle))
                        {
                            bundle = this.m_bundlePersistence.Insert(bundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                        }
                    }
                    else if (e.Data is Act actData)
                    {
                        throw new NotImplementedException("Acts on MDM ROT not implemented");
                    }
                    e.Cancel = true;
                }
                else if (e.Data is IClassifiable classifiable && classifiable.ClassConceptKey != MdmConstants.MasterRecordClassification) // record is a local and may need to be re-matched
                {
                    // Perform matching
                    var bundle = this.PerformMdmMatch(e.Data);
                    bundle.Item.InsertRange(0, this.RefactorMdmTargetsToLocals(e.Data, bundle));
                    e.Cancel = true;

                    // Is the caller the bundle MDM? if so just add
                    if (sender is Bundle bundleS)
                    {
                        bundleS.Item.InsertRange(bundleS.Item.FindIndex(o => o.Key == e.Data.Key), bundle.Item.Where(o => o != e.Data));
                    }
                    else
                    {
                        // Manually fire the business rules trigger for Bundle
                        var businessRulesService = ApplicationServiceContext.Current.GetService<IBusinessRulesService<Bundle>>();
                        bundle = businessRulesService?.BeforeUpdate(bundle) ?? bundle;
                        // Business rules shouldn't be used for relationships, we need to delay load the sources
                        bundle.Item.OfType<EntityRelationship>().ToList().ForEach((i) =>
                        {
                            if (i.SourceEntity == null)
                            {
                                var candidate = bundle.Item.Find(o => o.Key == i.SourceEntityKey) as Entity;
                                if (candidate != null)
                                    i.SourceEntity = candidate;
                            }
                        });
                        bundle = this.m_bundlePersistence.Update(bundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                        bundle = businessRulesService?.AfterUpdate(bundle) ?? bundle;
                    }
                    //ApplicationServiceContext.Current.GetService<IThreadPoolService>().QueueUserWorkItem(this.PerformMdmMatch, identified);
                }
            }
            catch (Exception ex)
            {
                throw new MdmException(e.Data, $"Error executing SAVE trigger for MDM on {e.Data}", ex);
            }
        }

        /// <summary>
        /// Determine whether the specified <paramref name="data"/> represents a ROT
        /// </summary>
        private bool IsRecordOfTruth(T data)
        {
            if (data is Entity entityData)
            {
                return entityData.Tags?.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true ||
                    entityData.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer ||
                    ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Count(o => o.TargetEntityKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal) > 0;
            }
            else if (data is Act actData)
            {
                this.m_traceSource.TraceUntestedWarning();
                return actData.Tags?.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true ||
                    ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Count(o => o.TargetActKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal) > 0;
            }
            else
                throw new ArgumentOutOfRangeException("Only Acts and Entities can be ROT");
        }

        /// <summary>
        /// Fired after record is inserted
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks>This method will fire the record matching service and will ensure that duplicates are marked
        /// and merged into any existing MASTER record.</remarks>
        protected virtual void OnInserting(object sender, DataPersistingEventArgs<T> e)
        {
            try
            {
                // Only the server is allowed to establish master records , clients are not permitted
                if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client ||
                    ApplicationServiceContext.Current.HostType == SanteDBHostType.Gateway)
                    return;

                // Already processed
                if (e.Data is ITaggable taggable)
                {
                    if (taggable.Tags.Any(o => o.TagKey == "$mdm.processed"))
                        return;
                    else
                        taggable.AddTag("$mdm.processed", "true");
                }

                if (e.Data.Key.GetValueOrDefault() == Guid.Empty)
                    e.Data.Key = Guid.NewGuid(); // Assign a key if one is not set
                                                 // Is this object a ROT or MASTER, if it is then we do not perform any changes to re-binding
                if (this.IsRecordOfTruth(e.Data))
                {
                    // Record of truth, ensure we update the appropriate
                    if (e.Data is Entity entityData)
                    {
                        // Establish a master
                        var bundle = sender as Bundle ?? new Bundle();
                        if (!bundle.Item.Contains(e.Data))
                            bundle.Add(e.Data);

                        bundle.Item.InsertRange(0, this.RefactorMdmTargetsToLocals(e.Data, bundle));

                        // Get the MDM relationship as this record will point > MDM
                        var masterRelationship = this.GetRelationshipTargets(e.Data, MdmConstants.MasterRecordRelationship).OfType<EntityRelationship>().FirstOrDefault();
                        if (masterRelationship == null) // Attempt to load from DB
                        {
                            masterRelationship = entityData.Relationships.FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                            if (masterRelationship == null) // Oddly this ROT doesn't have a master, we should restore it
                            {
                                this.m_traceSource.TraceWarning("ROT does not belong to a master, adding link based on ROT relationship");
                                var rotRelationship = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.TargetEntityKey == e.Data.Key && o.ObsoleteVersionSequenceId == null && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship, 0, 2, out int _, AuthenticationContext.SystemPrincipal).SingleOrDefault();
                                if (rotRelationship == null)
                                    throw new MdmException(e.Data, $"Unable to determine master record for {e.Data}");
                                else
                                {
                                    masterRelationship = new EntityRelationship(MdmConstants.MasterRecordRelationship, rotRelationship.SourceEntityKey) { SourceEntityKey = rotRelationship.TargetEntityKey, ClassificationKey = MdmConstants.VerifiedClassification };
                                    bundle.Add(masterRelationship);
                                }
                            }
                        }

                        // Is there already a master of truth relationship for a different record for the master we're pointing at?
                        var existingRotRel = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.SourceEntityKey == masterRelationship.TargetEntityKey && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (existingRotRel != null)
                        {
                            // is it pointing at a different record?
                            if (existingRotRel.TargetEntityKey != entityData.Key)
                            {
                                this.m_traceSource.TraceUntestedWarning();
                                // Remove the old
                                existingRotRel.ObsoleteVersionSequenceId = Int32.MaxValue;
                                bundle.Add(existingRotRel);
                                // Add the new
                                bundle.Add(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, entityData.Key) { SourceEntityKey = masterRelationship.TargetEntityKey });
                            }
                        }
                        else
                            bundle.Add(new EntityRelationship(MdmConstants.MasterRecordOfTruthRelationship, entityData.Key) { SourceEntityKey = masterRelationship.TargetEntityKey });

                        // Is there already a master of truth relationship for this object which is a different master than the one specified?
                        if (ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Count(o => o.TargetEntityKey == e.Data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.SourceEntityKey != masterRelationship.TargetEntityKey && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal) > 0)
                        {
                            throw new InvalidOperationException("You're trying to set this record as the record of truth for two different masters. This is not allowed");
                        }

                        // Sender is not bundle so this isn't a chained process
                        if (!(sender is Bundle))
                        {
                            bundle = this.m_bundlePersistence.Insert(bundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                        }
                    }
                    else if (e.Data is Act actData)
                    {
                        throw new NotImplementedException("Acts on MDM ROT not implemented");
                    }
                    e.Cancel = true;
                }
                else if (e.Data is IClassifiable classifiable && classifiable.ClassConceptKey != MdmConstants.MasterRecordClassification) // record is a local and may need to be re-matched
                {
                    e.Cancel = true;
                    var bundle = this.PerformMdmMatch(e.Data);
                    bundle.Item.InsertRange(0, this.RefactorMdmTargetsToLocals(e.Data, bundle));

                    // Is the caller the bundle MDM? if so just add
                    if (sender is Bundle bundleS)
                    {
                        //(sender as Bundle).Item.Remove(e.Data);
                        bundleS.Item.InsertRange(bundleS.Item.FindIndex(o => o.Key == e.Data.Key), bundle.Item.Where(o => o != e.Data));
                    }
                    else
                    {
                        var businessRulesService = ApplicationServiceContext.Current.GetService<IBusinessRulesService<Bundle>>();
                        bundle = businessRulesService?.BeforeInsert(bundle) ?? bundle;
                        // Business rules shouldn't be used for relationships, we need to delay load the sources
                        bundle.Item.OfType<EntityRelationship>().ToList().ForEach((i) =>
                        {
                            if (i.SourceEntity == null)
                            {
                                var candidate = bundle.Item.Find(o => o.Key == i.SourceEntityKey) as Entity;
                                if (candidate != null)
                                    i.SourceEntity = candidate;
                            }
                        });
                        bundle = this.m_bundlePersistence.Insert(bundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                        bundle = businessRulesService?.AfterInsert(bundle) ?? bundle;
                    }
                    //ApplicationServiceContext.Current.GetService<IThreadPoolService>().QueueUserWorkItem(this.PerformMdmMatch, identified);
                }
            }
            catch (Exception ex)
            {
                throw new MdmException(e.Data, $"Error executing INSERT trigger for MDM on {e.Data}", ex);
            }
        }

        /// <summary>
        /// Refactor any targets which are under MDM control to locals
        /// </summary>
        /// <remarks>
        /// When an local is submitted from a client, the client may be referencing an invalid relationship. This occurs, for example,
        /// when a client registers a local record however wants to link the target of that record to an MDM master. Meaning that a Patient
        /// may have a Mother which is a MDM-Master classification rather than a Patient or Person classification.
        ///
        /// This is not permitted and will fail validation. This method will, instead, check if the target of any relationships
        /// are masters, and if so, will locate or create a local for the target as well as a placeholder.
        /// </remarks>
        private IEnumerable<IdentifiedData> RefactorMdmTargetsToLocals(IIdentifiedEntity data, Bundle sourceBundle)
        {
            var retVal = new List<IdentifiedData>();

            if (data is Entity entity)
            {
                // Get those target relationships which point at an MDM managed type
                var mdmTargetRels = entity.GetRelationships().Where((rel) =>
                {
                    var target = rel.GetTargetAs<Entity>();
                    return target != null &&
                        target.ClassConceptKey == MdmConstants.MasterRecordClassification &&
                        rel.RelationshipTypeKey != MdmConstants.CandidateLocalRelationship &&
                        rel.RelationshipTypeKey != MdmConstants.MasterRecordRelationship &&
                        rel.RelationshipTypeKey != MdmConstants.OriginalMasterRelationship &&
                        rel.RelationshipTypeKey != MdmConstants.MasterRecordOfTruthRelationship;
                });

                // Now, we want to point these rels at the existing local , or we want to create a local
                foreach (var rel in mdmTargetRels)
                {
                    var local = this.GetLocalFor(rel.GetTargetAs<Entity>(), AuthenticationContext.Current.Principal as IClaimsPrincipal) as Entity;

                    // Search in the bundle
                    if (local == null && sourceBundle != null)
                        local = sourceBundle.Item.OfType<Entity>().FirstOrDefault(o => o.Relationships.Any(r => r.RelationshipTypeKey == MdmConstants.MasterRecordRelationship && r.TargetEntityKey == rel.TargetEntityKey));

                    if (local == null) // There is no local :/ so we have to create one
                    {
                        // Synthesize a master or ROT
                        var master = new EntityMaster<T>(rel.TargetEntity);
                        local = new T() as Entity;
                        local.Key = Guid.NewGuid(); // New key
                        local.VersionKey = Guid.NewGuid();
                        local.VersionSequence = null;
                        local.SemanticCopy((Entity)(object)master.GetMaster(AuthenticationContext.SystemPrincipal));
                        local.StripAssociatedItemSources();
                        local.Relationships.Clear();
                        local.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, rel.TargetEntity)
                        {
                            ClassificationKey = MdmConstants.VerifiedClassification // Someone is explicitly setting this
                        });
                        retVal.Add(local);
                    }
                    rel.TargetEntityKey = local.Key;
                }
            }

            return retVal;
        }

        /// <summary>
        /// Performs a match based on identity
        /// </summary>
        private IEnumerable<IRecordMatchResult<T>> PerformIdentityMatch(T entity)
        {
            if (this.m_uniqueAuthorities == null)
                this.m_uniqueAuthorities = ApplicationServiceContext.Current.GetService<IDataPersistenceService<AssigningAuthority>>()
                    .Query(o => o.IsUnique, AuthenticationContext.SystemPrincipal);

            // Identifiers in which entity has the unique authority
            var uqIdentifiers = (typeof(T).GetProperty(nameof(Entity.Identifiers)).GetValue(entity) as IEnumerable)?.OfType<IExternalIdentifier>();

            if (uqIdentifiers?.Any(i => i.Authority == null) == true)
                throw new InvalidOperationException("Some identifiers are missing authorities, cannot perform identity match");

            // TODO: Build this using Expression trees rather than relying on the parsing methods
            if (uqIdentifiers?.Any() != true)
                return new List<IRecordMatchResult<T>>();
            else
            {
                NameValueCollection nvc = new NameValueCollection();
                foreach (var itm in uqIdentifiers.Where(o => this.m_uniqueAuthorities.Any(u => o.Authority?.Key == u.Key || o.Authority?.DomainName == u.DomainName)))
                    nvc.Add($"identifier[{itm.Authority?.Key?.ToString() ?? itm.Authority?.DomainName}].value", itm.Value);
                var filterExpression = QueryExpressionParser.BuildLinqExpression<T>(nvc);

                // Now we want to filter returning the masters
                using (AuthenticationContext.EnterSystemContext())
                {
                    return this.m_repository.Find(filterExpression).Select(o => new MdmIdentityMatchResult<T>(o));
                }
            }
        }

        /// <summary>
        /// Perform a provider match
        /// </summary>
        private IEnumerable<IRecordMatchResult<T>> PerformProviderMatch(T entity, String configurationName)
        {
            var matchService = ApplicationServiceContext.Current.GetService<IRecordMatchingService>();
            if (matchService == null)
                throw new InvalidOperationException("Cannot operate MDM mode without matching service"); // Cannot make determination of matching

            var rawMatches = matchService.Match(entity, configurationName);
            return rawMatches.ToArray(); // ToArray is to prevent multiple calls to the matching engine
        }

        /// <summary>
        /// Perform an MDM match process to link the probable and definitive match
        /// </summary>
        /// <param name="entity">The entity to perform the MDM match on</param>
        /// <param name="matchConfiguration">When provided, the names of the configurations to use to match</param>
        private Bundle PerformMdmMatch(T entity, params string[] matchConfiguration)
        {
            this.m_traceSource.TraceVerbose("{0} : MDM will perform candidate match for entity", entity);

            // Get the existing master
            var existingMasterKey = this.GetMaster(entity);
            var taggable = (ITaggable)entity;
            var relationshipType = entity is Entity ? typeof(EntityRelationship) : typeof(ActRelationship);
            var relationshipService = ApplicationServiceContext.Current.GetService(typeof(IDataPersistenceService<>).MakeGenericType(relationshipType)) as IDataPersistenceService;

            // Perform matches
            IEnumerable<IRecordMatchResult<T>> matchingRecords = null;
            if (matchConfiguration.Length > 0)
                matchingRecords = matchConfiguration.SelectMany(o => this.PerformProviderMatch(entity, o));
            else
                matchingRecords = this.PerformIdentityMatch(entity).Union(this.m_resourceConfiguration.MatchConfiguration.SelectMany(o => this.PerformProviderMatch(entity, o)));

            this.m_traceSource.TraceVerbose("{0} : Matching layer has identified {1} candidate(s)", entity, matchingRecords.Count());

            // Matching records can only match with those that have MASTER records
            var matchGroups = matchingRecords
                .Where(o => o.Record.Key != entity.Key)
                .Select(o => new MasterMatch(this.GetMaster(o.Record).Value, o))
                .Distinct(s_matchComparer)
                .GroupBy(o => o.MatchResult.Classification)
                .ToDictionary(o => o.Key, o => o.Distinct(s_matchComparer));

            if (!matchGroups.ContainsKey(RecordMatchClassification.Match))
                matchGroups.Add(RecordMatchClassification.Match, new List<MasterMatch>());
            if (!matchGroups.ContainsKey(RecordMatchClassification.Probable))
                matchGroups.Add(RecordMatchClassification.Probable, new List<MasterMatch>());

            this.m_traceSource.TraceVerbose("{0} : Matching layer has identified {1} matches exceeding configured threshold for definitive match and {2} probable matches", entity, matchGroups[RecordMatchClassification.Match]?.Count(), matchGroups[RecordMatchClassification.Probable]?.Count());

            // Record is a LOCAL record
            //// INPUT = INBOUND LOCAL RECORD (FROM PATIENT SOURCE) THAT HAS BEEN INSERTED
            //// MATCHES = THE RECORDS THAT HAVE BEEN DETERMINED TO BE DEFINITE MATCHES WHEN COMPARED TO INPUT
            //// PROBABLES = THE RECORDS THAT HAVE BEEN DETERMINED TO BE POTENTIAL MATCHES WHEN COMPARE TO INPUT
            //// AUTOMERGE = A CONFIGURATION VALUE WHICH INSTRUCTS THE MPI TO AUTOMATICALLY MERGE DATA WHEN SAFE

            //// THE MATCH SERVICE HAS FOUND 1 DEFINITE MASTER RECORD THAT MATCHES THE INPUT RECORD AND AUTOMERGE IS ON
            //IF MATCHES.COUNT = 1 AND AUTOMERGE = TRUE THEN
            //        INPUT.MASTER = MATCHES[0]; // ASSIGN THE FOUND MATCH AS THE MASTER RECORD OF THE INPUT
            //// THE MATCH SERVICE HAS FOUND NO DEFINITE MASTER RECORDS THAT MATCH THE INPUT RECORD OR 1 WAS FOUND AND AUTOMERGE IS OFF
            //ELSE
            //        INPUT.MASTER = NEW MASTER(INPUT) // CREATE A NEW MASTER RECORD FOR THE INPUT
            //        FOR EACH MATCH IN MATCHES // FOR EACH OF THE DEFINITE MATCHES THAT WERE FOUND ADD THEM AS PROBABLE MATCHES TO THE INPUT
            //            INPUT.PROBABLE.ADD(MATCH);
            //        END FOR
            //END IF

            //// ANY PROBABLE MATCHES FROM THE MATCH SERVICE ARE JUST ADDED AS PROBABLES
            //FOR EACH PROB IN PROBABLES
            //        INPUT.PROBABLE.ADD(PROB);
            //END FOR
            List<IdentifiedData> insertData = new List<IdentifiedData>() { entity };
            var ignoreRelationships = this.GetRelationshipTargets(entity, MdmConstants.IgnoreCandidateRelationship).Distinct();
            var ignoreList = ignoreRelationships.OfType<EntityRelationship>().Select(o => o.TargetEntityKey).Concat(ignoreRelationships.OfType<ActRelationship>().Select(o => o.TargetActKey)).ToList();

            // Existing probable links
            var existingProbableLinks = this.GetRelationshipTargets(entity, MdmConstants.CandidateLocalRelationship);
            // We want to obsolete any existing links that are no longer valid
            foreach (var er in existingProbableLinks.OfType<EntityRelationship>().Where(er => matchGroups[RecordMatchClassification.Match]?.Any(m => m.Master == er.TargetEntityKey) == false && matchGroups[RecordMatchClassification.Probable]?.Any(m => m.Master == er.TargetEntityKey) == false))
            {
                er.ObsoleteVersionSequenceId = Int32.MaxValue;
                insertData.Add(er);
            }
            foreach (var ar in existingProbableLinks.OfType<ActRelationship>().Where(ar => matchGroups[RecordMatchClassification.Match]?.Any(m => m.Master == ar.TargetActKey) == false && matchGroups[RecordMatchClassification.Probable]?.Any(m => m.Master == ar.TargetActKey) == false))
            {
                ar.ObsoleteVersionSequenceId = Int32.MaxValue;
                insertData.Add(ar);
            }

            // There is exactly one match and it is set to automerge
            if (matchGroups.ContainsKey(RecordMatchClassification.Match) && matchGroups[RecordMatchClassification.Match]?.Count() == 1
                && (this.m_resourceConfiguration.AutoMerge || matchGroups[RecordMatchClassification.Match].First().MatchResult.Method == RecordMatchMethod.Identifier))
            {
                // Next, ensure that the new master is set
                this.m_traceSource.TraceVerbose("{0}: Entity has exactly 1 exact match and the configuration indicates auto-merge", entity);
                var masterMatch = matchGroups[RecordMatchClassification.Match].Single();

                if (masterMatch.Master != existingMasterKey)
                {
                    // change in exact match to another master
                    // We want to remove all previous master matches
                    var rels = this.GetRelationshipTargets(entity, MdmConstants.MasterRecordRelationship);

                    // Are there any matches, then obsolete them and redirect to this master
                    foreach (var r in rels)
                    {
                        this.m_traceSource.TraceVerbose("{0}: Removing an original MDM Master relationship {1}", entity, r);

                        // Assign source entity
                        if (r is EntityRelationship)
                            (r as EntityRelationship).SourceEntity = entity as Entity;
                        else
                            (r as ActRelationship).SourceEntity = entity as Act;

                        if (r is EntityRelationship)
                            (r as EntityRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;
                        else
                            (r as ActRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;

                        // Cleanup old master
                        var oldMasterId = (Guid)r.GetType().GetQueryProperty("target").GetValue(r);

                        // Keep track of the old master
                        insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.OriginalMasterRelationship, entity.Key, oldMasterId, MdmConstants.VerifiedClassification));

                        var query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMasterId}&source=!{entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        relationshipService.Query(query, 0, 0, out int tr);
                        if (tr == 0) // no other records point at the old master, obsolete it
                        {
                            var idt = typeof(IDataPersistenceService<>).MakeGenericType(typeof(Entity).IsAssignableFrom(typeof(T)) ? typeof(Entity) : typeof(Act));
                            var ids = ApplicationServiceContext.Current.GetService(idt) as IDataPersistenceService;
                            var oldMaster = ids.Get(oldMasterId) as IdentifiedData;
                            this.m_traceSource.TraceVerbose("{0}: The old master relationship {1} is no longer valid, and contains no other locals. Will obsolete master {2}", entity, r, oldMaster);
                            (oldMaster as IHasState).StatusConceptKey = StatusKeys.Obsolete;
                            insertData.Add(oldMaster);
                        }
                        insertData.Add(r as IdentifiedData);
                    }

                    insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity.Key, masterMatch.Master, MdmConstants.AutomagicClassification));
                }
                // dataService.Update(master);
                // No change in master
            }
            else
            {
                // We want to create a new master for this record?
                var rels = this.GetRelationshipTargets(entity, MdmConstants.MasterRecordRelationship);

                if (!existingMasterKey.HasValue) // There is no master
                {
                    this.m_traceSource.TraceVerbose("{0}: Entity has no existing master record. Creating one.", entity);
                    var master = this.CreateMasterRecord();
                    if (master is Entity masterEntity && entity is Entity localEntity)
                        masterEntity.DeterminerConceptKey = localEntity.DeterminerConceptKey;
                    else if (master is Act masterAct && entity is Act localAct)
                        masterAct.MoodConceptKey = localAct.MoodConceptKey;

                    insertData.Add(master as IdentifiedData);
                    insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity.Key, master.Key, MdmConstants.AutomagicClassification));
                }
                else if (!matchGroups[RecordMatchClassification.Match].Any() || !matchGroups[RecordMatchClassification.Match].Any(o => o.Master == existingMasterKey)) // No match with the existing master => Redirect the master
                {
                    // Is this the only record in the current master relationship?
                    var oldMasterRel = rels.OfType<IdentifiedData>().SingleOrDefault()?.Clone();

                    if (oldMasterRel != null && // Master in DB
                        (oldMasterRel is EntityRelationship erMaster && erMaster.ClassificationKey != MdmConstants.VerifiedClassification) // Master rel is not "sicky"
                    )
                    {
                        var oldMasterId = (Guid)oldMasterRel.GetType().GetQueryProperty("target").GetValue(oldMasterRel);
                        ApplicationServiceContext.Current.GetService<IDataCachingService>()?.Remove(oldMasterRel.Key.Value);

                        // Query for other masters
                        var query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMasterId}&source=!{entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        relationshipService.Query(query, 0, 0, out int tr);
                        if (tr > 0) // Old master has other records, we want to obsolete our current reference to it and then establish a new master
                        {
                            var master = this.CreateMasterRecord();
                            if (master is Entity masterEntity && entity is Entity localEntity)
                                masterEntity.DeterminerConceptKey = localEntity.DeterminerConceptKey;
                            else if (master is Act masterAct && entity is Act localAct)
                                masterAct.MoodConceptKey = localAct.MoodConceptKey;

                            insertData.Add(master as IdentifiedData);
                            insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity.Key, master.Key, MdmConstants.AutomagicClassification));
                            if (oldMasterRel is EntityRelationship)
                                (oldMasterRel as EntityRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;
                            else
                                (oldMasterRel as ActRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;
                            insertData.Insert(0, oldMasterRel);
                            insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.OriginalMasterRelationship, entity.Key, oldMasterId, erMaster.ClassificationKey));
                            this.m_traceSource.TraceVerbose("{0}: Old master record still hase other locals, creating new master {1} and detaching old relationship {2}", entity, master, oldMasterRel);
                        }
                        // If not we want to keep our link to the current master
                        else
                        {
                            insertData.Add(oldMasterRel);
                            this.m_traceSource.TraceVerbose("{0}: Entity was identified as not matching its current MASTER however the old master only has one local so we'll keep it", entity);
                        }
                    }
                    else  // Remove any candidate to the existing master
                    {
                        if (rels.Any()) // Reuse master rels
                            insertData.Add(rels.OfType<IdentifiedData>().SingleOrDefault());
                        ignoreList.Add(existingMasterKey);
                    }
                }
                else if (rels.Any()) // no change in master so just reuse rels from DB
                    insertData.Add(rels.OfType<IdentifiedData>().SingleOrDefault());

                var nonMasterMatches = matchGroups[RecordMatchClassification.Match]?.Where(o => o.Master != existingMasterKey);

                // Direct matches become candidate records
                if (nonMasterMatches.Any())
                {
                    // Get existing candidate locals (we don't want to report twice)
                    var query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                        .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={entity.Key}&relationshipType={MdmConstants.CandidateLocalRelationship}") }) as Expression;
                    rels = relationshipService.Query(query, 0, 100, out int tr).OfType<IdentifiedData>();

                    // Add any NEW match data which we didn't know about before
                    insertData.AddRange(nonMasterMatches
                        .Where(m => !ignoreList.Contains(m.Master))
                        .Select(m => this.CreateRelationship(relationshipType, MdmConstants.CandidateLocalRelationship, entity.Key, m.Master, MdmConstants.AutomagicClassification)));

                    // Remove all rels which don't appear in the insert data
                    foreach (var r in rels.OfType<IdentifiedData>())
                        if (!nonMasterMatches.Any(a => a.Master == (r as EntityRelationship)?.TargetEntityKey || a.Master == (r as ActRelationship)?.TargetActKey))
                            (r as IVersionedAssociation).ObsoleteVersionSequenceId = Int32.MaxValue;
                        else
                            insertData.Add(r);
                }
            }

            // Add probable records
            if (matchGroups[RecordMatchClassification.Probable] != null)
                insertData.AddRange(matchGroups[RecordMatchClassification.Probable]
                    .Where(m => !ignoreList.Contains(m.Master)) // ignore list
                    .Select(m => this.CreateRelationship(relationshipType, MdmConstants.CandidateLocalRelationship, entity.Key, m.Master, MdmConstants.AutomagicClassification)));

            // Now we want to make sure the relationships on the entity (provided) don't contain any tainted relationship data as we'll be returning this in the bundle
            if (entity is Entity entityData) entityData.Relationships.RemoveAll(o => insertData.OfType<EntityRelationship>().Any(i => i.RelationshipTypeKey == o.RelationshipTypeKey));
            else if (entity is Act actData) actData.Relationships.RemoveAll(o => insertData.OfType<ActRelationship>().Any(i => i.RelationshipTypeKey == o.RelationshipTypeKey));

            this.m_traceSource.TraceVerbose("{0}: MDM matching has identified {1} changes to be made to the accomodate new data", entity, insertData.Count);
            return new Bundle() { Item = insertData };
        }

        /// <summary>
        /// Get relationships for the entity of specified type
        /// </summary>
        private IEnumerable<IdentifiedData> GetRelationshipTargets(T sourceEntity, Guid relationshipType, bool inverse = false)
        {
            if (sourceEntity is Entity entity)
            {
                var erService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>();
                if (inverse)
                    return erService.Query(o => o.TargetEntityKey == sourceEntity.Key && o.RelationshipTypeKey == relationshipType, AuthenticationContext.SystemPrincipal).OfType<IdentifiedData>();
                else
                    return erService.Query(o => o.SourceEntityKey == sourceEntity.Key && o.RelationshipTypeKey == relationshipType, AuthenticationContext.SystemPrincipal).OfType<IdentifiedData>();
            }
            else if (sourceEntity is Act act)
            {
                var erService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>();
                if (inverse)
                    return erService.Query(o => o.TargetActKey == sourceEntity.Key && o.RelationshipTypeKey == relationshipType, AuthenticationContext.SystemPrincipal).OfType<IdentifiedData>();
                else
                    return erService.Query(o => o.SourceEntityKey == sourceEntity.Key && o.RelationshipTypeKey == relationshipType, AuthenticationContext.SystemPrincipal).OfType<IdentifiedData>();
            }
            else
                throw new InvalidOperationException("Cannot fetch associations for the specified type of source entity");
        }

        /// <summary>
        /// Get the master for the specified record
        /// </summary>
        private Guid? GetMaster(IdentifiedData match)
        {
            // Is the object already a master?
            Guid? retVal = null;
            var tag = (match as ITaggable)?.Tags.FirstOrDefault(o => o.TagKey == "$mdm.type"); // In case it was loaded from DB

            if (match is IClassifiable classifiable)
            {
                if (classifiable.ClassConceptKey == MdmConstants.MasterRecordClassification || tag?.Value == "M") // master
                    retVal = match.Key;
                else if (match is Entity entity)// Entity
                {
                    // Record of truth
                    if (entity.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer)
                        retVal = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.TargetEntityKey == match.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship, 0, 1, out int t, AuthenticationContext.SystemPrincipal).SingleOrDefault()?.SourceEntityKey;
                    else
                    {
                        var er = entity.Relationships.SingleOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                        if (er != null)
                            return er.TargetEntityKey;
                        else
                            retVal = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.SourceEntityKey == match.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, 0, 1, out int t, AuthenticationContext.SystemPrincipal).SingleOrDefault()?.TargetEntityKey;
                    }
                }
                else if (match is Act act) // Act
                {
                    if (act.MoodConceptKey == MdmConstants.RecordOfTruthDeterminer) // ROT
                        retVal = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.TargetActKey == match.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship, 0, 1, out int t, AuthenticationContext.SystemPrincipal).SingleOrDefault()?.SourceEntityKey;
                    else // local
                        retVal = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.SourceEntityKey == match.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, 0, 1, out int t, AuthenticationContext.SystemPrincipal).SingleOrDefault()?.TargetActKey;
                }
            }
            else
                throw new InvalidOperationException("Type should not be registered for MDM as it cannot be classified.");

            return retVal;
        }

        /// <summary>
        /// Create a relationship of the specied type
        /// </summary>
        /// <param name="relationshipType"></param>
        /// <param name="relationshipTypeConcept"></param>
        /// <param name="sourceEntity"></param>
        /// <param name="targetEntity"></param>
        /// <returns></returns>
        private IdentifiedData CreateRelationship(Type relationshipType, Guid relationshipTypeConcept, Guid? sourceEntity, Guid? targetEntity, Guid? classification)
        {
            var relationship = Activator.CreateInstance(relationshipType, relationshipTypeConcept, targetEntity) as IdentifiedData;
            relationship.Key = Guid.NewGuid();
            if (relationship is ITargetedAssociation targetAssociation)
            {
                targetAssociation.SourceEntityKey = sourceEntity;
                targetAssociation.ClassificationKey = classification;
            }
            return relationship;
        }

        /// <summary>
        /// Create a master record from the specified local records
        /// </summary>
        /// <param name="localRecords">The local records that are to be used to generate a master record</param>
        /// <returns>The created master record</returns>
        private IMdmMaster<T> CreateMasterRecord()
        {
            var mtype = typeof(Entity).IsAssignableFrom(typeof(T)) ? typeof(EntityMaster<>) : typeof(ActMaster<>);
            var retVal = Activator.CreateInstance(mtype.MakeGenericType(typeof(T))) as IMdmMaster<T>;
            retVal.Key = Guid.NewGuid();
            retVal.VersionKey = null;
            (retVal as BaseEntityData).CreatedByKey = Guid.Parse(AuthenticationContext.SystemApplicationSid);
            return retVal;
        }

        /// <summary>
        /// Dispose this object (unsubscribe)
        /// </summary>
        public override void Dispose()
        {
            if (this.m_repository != null)
            {
                this.m_repository.Inserting -= this.OnPrePersistenceValidate;
                this.m_repository.Saving -= this.OnPrePersistenceValidate;
                this.m_repository.Inserting -= this.OnInserting;
                this.m_repository.Saving -= this.OnSaving;
                this.m_repository.Retrieved -= this.OnRetrieved;
                this.m_repository.Retrieving -= this.OnRetrieving;
                this.m_repository.Obsoleting -= this.OnObsoleting;
                this.m_repository.Querying -= this.OnQuerying;
                this.m_repository.Queried -= this.OnQueried;
            }
        }

        /// <summary>
        /// Instructs the MDM service to merge the specified master with the linked duplicates
        /// </summary>
        /// <param name="master"></param>
        /// <param name="linkedDuplicates"></param>
        /// <returns></returns>
        public virtual T Merge(Guid suvivorKey, IEnumerable<Guid> linkedDuplicates)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                DataMergingEventArgs<T> preEventArgs = new DataMergingEventArgs<T>(suvivorKey, linkedDuplicates);
                this.Merging?.Invoke(this, preEventArgs);
                if (preEventArgs.Cancel)
                {
                    this.m_traceSource.TraceInfo("Pre-event handler has indicated a cancel of merge on {0}", suvivorKey);
                    return null;
                }

                suvivorKey = preEventArgs.SurvivorKey; // Allow resource to update these fields
                linkedDuplicates = preEventArgs.LinkedKeys;

                // Relationship type
                var relationshipType = typeof(Entity).IsAssignableFrom(typeof(T)) ? typeof(EntityRelationship) : typeof(ActRelationship);
                var relationshipService = ApplicationServiceContext.Current.GetService(typeof(IDataPersistenceService<>).MakeGenericType(relationshipType)) as IDataPersistenceService;

                // Ensure that MASTER is in fact a master
                var survivorData = this.m_rawMasterPersistenceService.Get(suvivorKey) as IdentifiedData;
                if (survivorData is IClassifiable classifiable && classifiable.ClassConceptKey == MdmConstants.MasterRecordClassification && !AuthenticationContext.Current.Principal.IsInRole("SYSTEM"))
                {
                    try
                    {
                        ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
                    }
                    catch (PolicyViolationException e) when (e.PolicyId == MdmPermissionPolicyIdentifiers.WriteMdmMaster)
                    {
                        survivorData = this.GetLocalFor(survivorData, AuthenticationContext.Current.Principal as IClaimsPrincipal) as IdentifiedData;
                        if (survivorData == null) // Exception: The entity is attempting to merge without MASTER permission and without a LOCAL
                                                  //             This means that we cannot find a permissible survivor into which the data can be merged
                        {
                            throw;
                        }
                    }
                }
                else // The caller explicitly called this functoin with a LOCAL UUID
                {
                    this.EnsureProvenance(survivorData as BaseEntityData, AuthenticationContext.Current.Principal);
                }

                // Validate survivor is in a known state
                if (!(survivorData is IHasState survivorState) || !this.m_mergeStates.Contains(survivorState.StatusConceptKey.GetValueOrDefault()))
                {
                    throw new InvalidOperationException($"Record {survivorData.Key} cannot be merged in its current state");
                }

                Bundle mergeTransaction = new Bundle();

                var survivorClassified = survivorData as IClassifiable;

                // For each of the linked duplicates we want to get the master relationships
                foreach (var ldpl in linkedDuplicates)
                {
                    // Is the linked duplicate a master record?
                    var linkedData = this.m_rawMasterPersistenceService.Get(ldpl) as IdentifiedData;
                    if (linkedData is IClassifiable classifiableLocal && classifiableLocal.ClassConceptKey == MdmConstants.MasterRecordClassification && !AuthenticationContext.Current.Principal.IsInRole("SYSTEM"))
                    {
                        try
                        {
                            ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.MergeMdmMaster);
                        }
                        catch (PolicyViolationException e) when (e.PolicyId == MdmPermissionPolicyIdentifiers.MergeMdmMaster)
                        {
                            linkedData = this.GetLocalFor(linkedData as IIdentifiedEntity, AuthenticationContext.Current.Principal as IClaimsPrincipal) as IdentifiedData;
                            if (linkedData == null) // Exception: The entity is attempting to merge without MASTER permission and without a LOCAL
                                                    //             This means that we cannot find a permissible survivor into which the data can be merged
                            {
                                throw;
                            }
                        }
                    }
                    else
                    {
                        this.EnsureProvenance(linkedData as BaseEntityData, AuthenticationContext.Current.Principal);
                    }

                    var linkedClassified = linkedData as IClassifiable;

                    // Validate linked is in a valid state
                    if (!(linkedData is IHasState linkedState) || !this.m_mergeStates.Contains(linkedState.StatusConceptKey.GetValueOrDefault()))
                    {
                        throw new InvalidOperationException($"Record {linkedData.Key} cannot be merged in its current state");
                    }

                    // Allowed merges
                    // LOCAL > MASTER - A local record is being merged into a MASTER
                    // MASTER > MASTER - Two MASTER records are being merged (administrative merge)
                    // LOCAL > LOCAL - Two LOCAL records are being merged

                    // Alternate Use Cases to be Detected;
                    //  - Survivor is OBSOLETE (needs to be re-activated)
                    //  - Linked marked as replacing survivor (previous reverse merge performed)
                    if (linkedClassified.ClassConceptKey == survivorClassified.ClassConceptKey)
                    {
                        if (linkedClassified.ClassConceptKey == MdmConstants.MasterRecordClassification) // MASTER <> MASTER
                        {
                            throw new NotSupportedException("Currently MASTER > MASTER merging is not supported");
                        }
                        else // LOCAL > LOCAL (we're rewriting the master relationship)
                        {
                            // First, is there a replaces relationship between the two?
                            if (survivorData is Entity survivorEntity &&
                                linkedData is Entity linkedEntity)
                            {
                                // The linked object must be obsoleted
                                linkedState.StatusConceptKey = StatusKeys.Obsolete;
                                // The master relationship for the linked state is removed (i.e. detached)
                                var linkedMasterRelationship = linkedEntity.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);

                                mergeTransaction.Add(new EntityRelationship(MdmConstants.OriginalMasterRelationship, linkedMasterRelationship.TargetEntityKey)
                                {
                                    SourceEntityKey = linkedEntity.Key,
                                    ClassificationKey = MdmConstants.VerifiedClassification
                                });
                                linkedMasterRelationship.ObsoleteVersionSequenceId = Int32.MaxValue;

                                // TODO: Verify we want to do this
                                // Rewrite the master to the survivor's master (helps with synthesis)
                                var survivorMasterRelationship = survivorEntity.LoadCollection(o => o.Relationships).FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
                                mergeTransaction.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, survivorMasterRelationship.TargetEntityKey)
                                {
                                    SourceEntityKey = linkedEntity.Key,
                                    ClassificationKey = MdmConstants.VerifiedClassification,
                                    RelationshipRoleKey = EntityRelationshipTypeKeys.Duplicate // TODO: Should this be its own code? Like MERGE TARGET?
                                });
                                mergeTransaction.Add(new EntityRelationship(EntityRelationshipTypeKeys.Replaces, linkedEntity.Key)
                                {
                                    SourceEntityKey = survivorEntity.Key,
                                    RelationshipRoleKey = EntityRelationshipTypeKeys.Duplicate
                                });

                                // Copy the identifiers over
                                survivorEntity.Identifiers.AddRange(linkedEntity.Identifiers.Where(lid => !survivorEntity.Identifiers.Any(sid => sid.SemanticEquals(lid)))
                                    .Select(o => new EntityIdentifier(o.Authority, o.Value)
                                    {
                                        IssueDate = o.IssueDate,
                                        ExpiryDate = o.ExpiryDate
                                    }));

                                mergeTransaction.Add(survivorEntity);
                                mergeTransaction.Add(linkedEntity);

                                // Clean up the old linked entity master
                                if (this.GetRelationshipTargets(new T() { Key = linkedMasterRelationship.TargetEntityKey.Value }, linkedMasterRelationship.RelationshipTypeKey.Value, true).Count(o => o.Key != linkedMasterRelationship.Key) == 0)
                                {
                                    var existingMaster = this.m_rawMasterPersistenceService.Get(linkedMasterRelationship.TargetEntityKey.Value) as Entity;
                                    existingMaster.StatusConceptKey = StatusKeys.Obsolete;
                                    mergeTransaction.Add(existingMaster);

                                    // TODO: Do we want this behavior?
                                    // Migrate master identifiers over and mark as replaced, and set their expiration date to now
                                    mergeTransaction.Item.AddRange(existingMaster.Identifiers.Where(lid => !survivorEntity.Identifiers.Any(sid => sid.SemanticEquals(lid)))
                                        .Select(o => new EntityIdentifier(o.Authority, o.Value)
                                        {
                                            SourceEntityKey = survivorMasterRelationship.TargetEntityKey,
                                            IssueDate = o.IssueDate,
                                            ExpiryDate = DateTime.Now
                                        }));

                                    // Mark replaced by
                                    mergeTransaction.Add(new EntityRelationship(EntityRelationshipTypeKeys.Replaces, survivorMasterRelationship.TargetEntityKey)
                                    {
                                        SourceEntityKey = existingMaster.Key,
                                        RelationshipRoleKey = EntityRelationshipTypeKeys.Duplicate
                                    });
                                }
                            }
                            else
                            {
                                // TODO: ^^ Refactor above to handle ACTS
                                throw new InvalidOperationException("Merging non entities is not supported yet");
                            }
                        }
                    }
                    // LOCAL > MASTER (re-link)
                    else if (survivorClassified.ClassConceptKey == MdmConstants.MasterRecordClassification &&
                        linkedClassified.ClassConceptKey != MdmConstants.MasterRecordClassification)
                    {
                        // First, get the relationship of CANDIDATE between the master and the linked duplicate
                        var relationshipQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={ldpl}&relationshipType={MdmConstants.CandidateLocalRelationship}&target={suvivorKey}") }) as Expression;
                        int tr = 0;
                        var candidateRelationship = relationshipService.Query(relationshipQry, 0, 2, out tr).OfType<IVersionedAssociation>().SingleOrDefault();
                        if (candidateRelationship != null)
                        {
                            candidateRelationship.ObsoleteVersionSequenceId = Int32.MaxValue;
                            mergeTransaction.Add(candidateRelationship as IdentifiedData);
                        }

                        // Next We want to add a new master record relationship between duplicate and the master
                        mergeTransaction.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, ldpl, suvivorKey, MdmConstants.VerifiedClassification));

                        var existingMasterKey = this.GetMaster(linkedClassified as IdentifiedData);

                        // Next , if the old master has no more locals, we want to obsolete it
                        relationshipQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source=!{ldpl}&target={existingMasterKey}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        relationshipService.Query(relationshipQry, 0, 1, out tr);

                        if (tr == 0) // No more locals, obsolete
                        {
                            var existingMaster = this.m_rawMasterPersistenceService.Get(existingMasterKey.Value) as IHasState;
                            existingMaster.StatusConceptKey = StatusKeys.Obsolete;
                            mergeTransaction.Add(existingMaster as IdentifiedData);
                        }
                    }
                    else
                        throw new InvalidOperationException("Invalid merge. Only LOCAL>MASTER, MASTER>MASTER or LOCAL>LOCAL are supported");
                }

                this.m_bundlePersistence.Insert(mergeTransaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);

                this.Merged?.Invoke(this, new DataMergeEventArgs<T>(suvivorKey, linkedDuplicates));
                if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    return new EntityMaster<T>(this.m_rawMasterPersistenceService.Get(suvivorKey) as Entity).GetMaster(AuthenticationContext.Current.Principal);
                else
                    return new ActMaster<T>(this.m_rawMasterPersistenceService.Get(suvivorKey) as Act).GetMaster(AuthenticationContext.Current.Principal);
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = suvivorKey }, $"Error merging records {String.Join(",", linkedDuplicates)} into {suvivorKey}", e);
            }
        }

        /// <summary>
        /// Ensures that <paramref name="master"/> is owned by application granted by <paramref name="principal"/>
        /// </summary>
        /// <param name="master"></param>
        /// <param name="principal"></param>
        private void EnsureProvenance(BaseEntityData master, IPrincipal principal)
        {
            var provenance = master.LoadProperty<SecurityProvenance>("CreatedBy");
            var claimsPrincipal = principal as IClaimsPrincipal;
            var applicationPrincipal = claimsPrincipal.Identities.OfType<IApplicationIdentity>().SingleOrDefault();
            if (applicationPrincipal != null &&
                applicationPrincipal.Name != provenance?.LoadProperty<SecurityApplication>("Application")?.Name // was not the original author
                || !AuthenticationContext.Current.Principal.IsInRole("SYSTEM"))
                ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.ReadMdmLocals);
        }

        /// <summary>
        /// Instructs the MDM service to unmerge the specified master from unmerge duplicate
        /// </summary>
        public virtual T Unmerge(Guid master, Guid unmergeDuplicate)
        {
            throw new NotImplementedException("Unmerge not yet implemented");
        }

        /// <summary>
        /// Ignore the specified false positives for the specified relationships
        /// </summary>
        public T Ignore(Guid masterKey, IEnumerable<Guid> falsePositives)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();
                Bundle obsoleteBundle = new Bundle();
                foreach (var key in falsePositives)
                {
                    if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    {
                        var er = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.SourceEntityKey == key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetEntityKey == masterKey, 0, 1, out int t, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (er != null)
                        {
                            er.ObsoleteVersionSequenceId = Int32.MaxValue;
                            obsoleteBundle.Add(er); // Obsolete the candidate relationship
                            obsoleteBundle.Add(this.CreateRelationship(typeof(EntityRelationship), MdmConstants.IgnoreCandidateRelationship, key, masterKey, MdmConstants.VerifiedClassification)); // Add an ignore to this LOCAL>MASTER candidate
                        }
                        else
                            throw new KeyNotFoundException($"Could not find relationship between {masterKey} and {key}");
                    }
                    else if (typeof(Act).IsAssignableFrom(typeof(T)))
                    {
                        var ar = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.SourceEntityKey == key && o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.TargetActKey == masterKey, 0, 1, out int t, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (ar != null)
                        {
                            ar.ObsoleteVersionSequenceId = Int32.MaxValue;
                            obsoleteBundle.Add(ar); // Obsolete the candidate relationship
                            obsoleteBundle.Add(this.CreateRelationship(typeof(ActRelationship), MdmConstants.IgnoreCandidateRelationship, key, masterKey, MdmConstants.VerifiedClassification)); // Add an ignore to this LOCAL>MASTER candidate
                        }
                        else
                            throw new KeyNotFoundException($"Could not find relationship between {masterKey} and {key}");
                    }
                }

                this.m_bundlePersistence.Insert(obsoleteBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);

                if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    return new EntityMaster<T>(this.m_rawMasterPersistenceService.Get(masterKey) as Entity).GetMaster(AuthenticationContext.Current.Principal);
                else
                    return new ActMaster<T>(this.m_rawMasterPersistenceService.Get(masterKey) as Act).GetMaster(AuthenticationContext.Current.Principal);
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = masterKey }, $"Error ignoring flagging false positives {String.Join(",", falsePositives)}", e);
            }
        }

        /// <summary>
        /// Get duplicates for the specified object
        /// </summary>
        public IEnumerable<T> GetDuplicates(Guid masterKey)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                // Relationship type
                Expression<Func<T, bool>> linqQuery = null;
                if (masterKey == Guid.Empty)
                    linqQuery = QueryExpressionParser.BuildLinqExpression<T>(NameValueCollection.ParseQueryString($"relationship[{MdmConstants.CandidateLocalRelationship}]=!null"));
                else
                    linqQuery = QueryExpressionParser.BuildLinqExpression<T>(NameValueCollection.ParseQueryString($"relationship[{MdmConstants.CandidateLocalRelationship}].target={masterKey}"));
                return this.m_dataPersistenceService.Query(linqQuery, AuthenticationContext.Current.Principal);
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = masterKey }, $"Error getting duplicates for {masterKey}", e);
            }
        }

        /// <summary>
        /// Create a patch between the master and the linked duplicate
        /// </summary>
        /// <param name="masterKey">The key of the master</param>
        /// <param name="linkedDuplicateKey">The key of the duplicate</param>
        /// <returns>The differences between the two</returns>
        public Patch Diff(Guid masterKey, Guid linkedDuplicateKey)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                var patchService = ApplicationServiceContext.Current.GetService<IPatchService>();
                if (patchService == null)
                    throw new InvalidOperationException("Cannot find patch service");

                var master = this.m_rawMasterPersistenceService.Get(masterKey);

                var linkedDuplicate = this.m_dataPersistenceService.Get(linkedDuplicateKey, null, AuthenticationContext.Current.Principal);
                if (master is Entity entityMaster)
                {
                    var retMaster = new EntityMaster<T>(entityMaster).GetMaster(AuthenticationContext.Current.Principal) as Entity;
                    retMaster.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship || o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship);
                    (linkedDuplicate as Entity).Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship || o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship);
                    return patchService.Diff(retMaster, linkedDuplicate);
                }
                else if (master is Act actMaster)
                {
                    return patchService.Diff(new ActMaster<T>(actMaster).GetMaster(AuthenticationContext.Current.Principal), linkedDuplicate);
                }
                else
                    throw new InvalidOperationException($"Cannot determine DIFF method for {master.GetType().Name}");
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = masterKey }, $"Error processing difference between master {masterKey} and linked duplicate {linkedDuplicateKey}", e);
            }
        }

        /// <summary>
        /// Get the ignored records
        /// </summary>
        /// <param name="masterKey">The master for which ignored records should be fetched</param>
        public IEnumerable<T> GetIgnored(Guid masterKey)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                // Relationship type
                var linqQuery = QueryExpressionParser.BuildLinqExpression<T>(NameValueCollection.ParseQueryString($"relationship[{MdmConstants.IgnoreCandidateRelationship}].target={masterKey}"));
                return this.m_dataPersistenceService.Query(linqQuery, AuthenticationContext.Current.Principal);
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = masterKey }, $"Error getting ignored entries for {masterKey}", e);
            }
        }

        /// <summary>
        /// Re-consider an ignored record for matching
        /// </summary>
        /// <remarks>This will re-run the matching on the master record</remarks>
        /// <param name="masterKey">The master key to re-consider</param>
        /// <param name="ignoredKeys">The ignored object</param>
        /// <returns>The </returns>
        public T UnIgnore(Guid masterKey, IEnumerable<Guid> ignoredKeys)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                Bundle obsoleteBundle = new Bundle();
                foreach (var key in ignoredKeys)
                {
                    if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    {
                        var er = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.SourceEntityKey == key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.TargetEntityKey == masterKey, 0, 1, out int t, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (er != null)
                        {
                            er.ObsoleteVersionSequenceId = Int32.MaxValue;
                            obsoleteBundle.Add(er); // Obsolete the candidate relationship
                        }
                        else
                            throw new KeyNotFoundException($"Could not find relationship between {masterKey} and {key}");
                    }
                    else if (typeof(Act).IsAssignableFrom(typeof(T)))
                    {
                        var ar = ApplicationServiceContext.Current.GetService<IDataPersistenceService<ActRelationship>>().Query(o => o.SourceEntityKey == key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.TargetActKey == masterKey, 0, 1, out int t, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        if (ar != null)
                        {
                            ar.ObsoleteVersionSequenceId = Int32.MaxValue;
                            obsoleteBundle.Add(ar); // Obsolete the candidate relationship
                        }
                        else
                            throw new KeyNotFoundException($"Could not find relationship between {masterKey} and {key}");
                    }

                    // Re-run the duplication detection
                    this.FlagDuplicates(key);
                }

                this.m_bundlePersistence.Insert(obsoleteBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    return new EntityMaster<T>(this.m_rawMasterPersistenceService.Get(masterKey) as Entity).GetMaster(AuthenticationContext.Current.Principal);
                else
                    return new ActMaster<T>(this.m_rawMasterPersistenceService.Get(masterKey) as Act).GetMaster(AuthenticationContext.Current.Principal);
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = masterKey }, $"Error un-ignoring false positives {String.Join(",", ignoredKeys)}", e);
            }
        }

        /// <summary>
        /// Calculate all duplicates for the specified configuration
        /// </summary>
        public void FlagDuplicates(string configurationName = null)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                ApplicationServiceContext.Current.GetService<IJobManagerService>().StartJob(this.m_backgroundMatch, new object[] { configurationName });
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not start calculation of duplicates", e);
            }
        }

        /// <summary>
        /// Removes all candidate duplicates and then re-runs the match configuration
        /// </summary>
        /// <param name="key">The master key to run the match on</param>
        /// <param name="configurationName">The name of the configuration to use for matching</param>
        /// <returns>The updated master duplicates</returns>
        public T FlagDuplicates(Guid key, string configurationName = null)
        {
            try
            {
                this.m_traceSource.TraceUntestedWarning();

                AuthenticationContext.Current = new AuthenticationContext(AuthenticationContext.SystemPrincipal);

                var localRecord = this.m_dataPersistenceService.Get(key, null, AuthenticationContext.SystemPrincipal);
                if (localRecord == null) // This is actually a master?
                {
                    // Run for candidates and related
                    if (typeof(Entity).IsAssignableFrom(typeof(T)))
                    {
                        foreach (var rel in ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.TargetEntityKey == key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship, AuthenticationContext.SystemPrincipal))
                            this.FlagDuplicates(rel.SourceEntityKey.Value, configurationName);
                        return localRecord;
                    }
                    else
                        throw new InvalidOperationException("Cannot run this operation on master records");
                }
                else if (this.IsRecordOfTruth(localRecord)) // Is this a ROT? if so we don't want to run matching on it either
                    throw new InvalidOperationException("Cannot run this operation on ROT records");

                // Clear out all MDM
                var matchBundle = this.PerformMdmMatch(localRecord, configurationName);
                matchBundle.Item.RemoveAll(o => o.Key == key); // Don't touch the source object

                // Manually fire the business rules trigger for Bundle
                var needsBre = !matchBundle.Item.All(o => o is EntityRelationship || o is ActRelationship);
                var businessRulesService = ApplicationServiceContext.Current.GetService<IBusinessRulesService<Bundle>>();

                if (needsBre)
                {
                    matchBundle = businessRulesService?.BeforeUpdate(matchBundle) ?? matchBundle;

                    // Business rules shouldn't be used for relationships, we need to delay load the sources
                    matchBundle.Item.OfType<EntityRelationship>().ToList().ForEach((i) =>
                    {
                        if (i.SourceEntity == null)
                        {
                            var candidate = matchBundle.Item.Find(o => o.Key == i.SourceEntityKey) as Entity;
                            if (candidate != null)
                                i.SourceEntity = candidate;
                        }
                    });
                }
                matchBundle = this.m_bundlePersistence.Update(matchBundle, TransactionMode.Commit, AuthenticationContext.Current.Principal);

                if (needsBre)
                    matchBundle = businessRulesService?.AfterUpdate(matchBundle) ?? matchBundle;

                return localRecord;
            }
            catch (Exception e)
            {
                throw new MdmException(new T() { Key = key }, $"Error running candidate for {key}", e);
            }
        }
    }
}