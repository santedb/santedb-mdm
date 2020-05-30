/*
 * Portions Copyright 2015-2019 Mohawk College of Applied Arts and Technology
 * Portions Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2020-2-2
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Model.Security;
using SanteDB.Core.Model.Serialization;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Security.Principal;
using SanteDB.Core.Security.Services;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

using System.Security.Permissions;
using System.Security.Principal;

namespace SanteDB.Persistence.MDM.Services
{

    /// <summary>
    /// Abstract wrapper for MDM resource listeners
    /// </summary>
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
        /// Gets the service name
        /// </summary>
        public string ServiceName => $"MIDM Data Handler Listener for {typeof(T).FullName}";

        // Configuration
        private ResourceMergeConfiguration m_resourceConfiguration;

        // Tracer
        private Tracer m_traceSource = new Tracer(MdmConstants.TraceSourceName);

        // The repository that this listener is attached to
        private INotifyRepositoryService<T> m_repository;

        // Persistence service
        private IDataPersistenceService<Bundle> m_bundlePersistence;

        // Persistence service
        private IDataPersistenceService<T> m_dataPersistenceService;

        /// <summary>
        /// Fired when the service is merging
        /// </summary>
        public event EventHandler<DataMergingEventArgs<T>> Merging;

        /// <summary>
        /// Fired when data has been merged
        /// </summary>
        public event EventHandler<DataMergeEventArgs<T>> Merged;

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

            this.m_repository = ApplicationServiceContext.Current.GetService<IRepositoryService<T>>() as INotifyRepositoryService<T>;
            if (this.m_repository == null)
                throw new InvalidOperationException($"Could not find repository service for {typeof(T)}");
            // Subscribe
            this.m_repository.Inserting += this.OnPrePersistenceValidate;
            this.m_repository.Saving += this.OnPrePersistenceValidate;
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
                    var localQuery = new NameValueCollection(query.ToDictionary(o => $"relationship[MDM-Master].source@{typeof(T).Name}.{o.Key}", o => o.Value));
                    localQuery.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
                    query.Add("classConcept", MdmConstants.MasterRecordClassification.ToString());
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
        private IEnumerable<T> MasterQuery<TMasterType>(NameValueCollection masterQuery, NameValueCollection localQuery, Guid queryId, int offset, int? count, IPrincipal principal, out int totalResults)
            where TMasterType : IdentifiedData
        {
            var qpi = ApplicationServiceContext.Current.GetService<IStoredQueryDataPersistenceService<TMasterType>>();
            IEnumerable<TMasterType> results = null;
            if (qpi is IUnionQueryDataPersistenceService<TMasterType> iqps)
            {
                // Try to do a linked query (unless the query is on a special local filter value)
                try
                {
                    var masterLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(masterQuery, null, false);
                    var localLinq = QueryExpressionParser.BuildLinqExpression<TMasterType>(localQuery, null, false);
                    results = iqps.Union(new Expression<Func<TMasterType, bool>>[] { masterLinq, localLinq }, queryId, offset, count, out totalResults, principal);
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
            // TODO: Filter master record data based on taboo child records.

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
            ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>().Query(o => o.Key == e.Id, 0, 0, out int records, AuthenticationContext.SystemPrincipal);
            if (records == 0) //
            {
                e.Cancel = true;
                if (typeof(Entity).IsAssignableFrom(typeof(T)))
                {
                    var master = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>().Get(e.Id.Value, null, false, AuthenticationContext.Current.Principal);
                    e.Result = new EntityMaster<T>(master).GetMaster(AuthenticationContext.Current.Principal);
                }
                else if (typeof(Act).IsAssignableFrom(typeof(T)))
                {
                    var master = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Act>>().Get(e.Id.Value, null, false, AuthenticationContext.Current.Principal);
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
                this.EnsureProvenance(e.Data, AuthenticationContext.Current.Principal);
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

            var idpType = typeof(IDataPersistenceService<>).MakeGenericType(e.Data is Entity ? typeof(Entity) : typeof(Act));
            var idp = ApplicationServiceContext.Current.GetService(idpType) as IDataPersistenceService;
            var identified = e.Data as IdentifiedData;
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
                        Entity existingLocal = null;
                        object identity = (AuthenticationContext.Current.Principal as IClaimsPrincipal)?.Identities.OfType<IDeviceIdentity>().FirstOrDefault() as Object ??
                            (AuthenticationContext.Current.Principal as IClaimsPrincipal)?.Identities.OfType<IApplicationIdentity>().FirstOrDefault() as Object;

                        if (mdmTag?.Value == "T" || dataEntity.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer) // Record of truth we must look for
                            existingLocal = (idp as IDataPersistenceService<Entity>).Query(o => o.ClassConceptKey == dataEntity.ClassConceptKey && o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-RecordOfTruth").Any(g => g.SourceEntityKey == existing.Key), 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        else if (identity is IDeviceIdentity deviceIdentity)
                            existingLocal = (idp as IDataPersistenceService<Entity>).Query(o => o.ClassConceptKey == dataEntity.ClassConceptKey && o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-Master").Any(g => g.TargetEntityKey == existing.Key) && o.CreatedBy.Device.Name == deviceIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();
                        else if (identity is IApplicationIdentity applicationIdentity)
                            existingLocal = (idp as IDataPersistenceService<Entity>).Query(o => o.ClassConceptKey == dataEntity.ClassConceptKey && o.Relationships.Where(g => g.RelationshipType.Mnemonic == "MDM-Master").Any(g => g.TargetEntityKey == existing.Key) && o.CreatedBy.Application.Name == applicationIdentity.Name, 0, 1, out int tr, AuthenticationContext.SystemPrincipal).FirstOrDefault();

                        // We're updating the existing local identity 
                        if (existingLocal != null)
                        {
                            dataEntity.VersionKey = null;
                            dataEntity.Key = existingLocal.Key;
                            existingLocal.CopyObjectData(dataEntity, true, true);
                            existingLocal.CopyObjectData(dataEntity, true, true); // HACK: Run again to clear out any delayed load properties
                            identified = e.Data = (T)(object)existingLocal;
                        }
                        else // We're creating a new local entity for this system
                        {
                            dataEntity.Relationships.RemoveAll(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship);
                            e.Data.Key = identified.Key = Guid.NewGuid(); // New key
                            dataEntity.VersionKey = Guid.NewGuid();
                            dataEntity.VersionSequence = null;
                            dataEntity.Tags.RemoveAll(o => o.TagKey == "$mdm.type" || o.TagKey.StartsWith("$"));

                            if (mdmTag?.Value == "T") // They want this record to be a record of truth 
                            {
                                dataEntity.DeterminerConceptKey = MdmConstants.RecordOfTruthDeterminer;
                                dataEntity.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, existing as Entity));
                                dataEntity.Tags.Add(new EntityTag("$mdm.type", "T"));
                            }
                            else // it is just another record
                                dataEntity.Relationships.Add(new EntityRelationship(MdmConstants.MasterRecordRelationship, existing as Entity));
                        }

                        dataEntity.StripAssociatedItemSources();

                    }
                }
                // Entity is not a master so don't do anything
            }

            // Data being persisted is an entity
            if (e.Data is Entity)
            {
                var eRelationship = (e.Data as Entity).LoadCollection<EntityRelationship>("Relationships").FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship);
                if (eRelationship != null)
                {
                    // Get existing er if available
                    var dbRelationship = ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Query(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship || o.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship && o.SourceEntityKey == identified.Key, 0, 1, out int tr, e.Principal);
                    if (tr == 0 || dbRelationship.First().TargetEntityKey == eRelationship.TargetEntityKey)
                        return;
                    else if (!e.Principal.IsInRole("SYSTEM")) // The target entity is being re-associated make sure the principal is allowed to do this
                        ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
                }

                if ((e.Data as ITaggable)?.Tags?.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true &&
                        (e.Data as Entity).Relationships?.Single(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship) == null)
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

            // We will receive an obsolete on a MASTER for its type however the repository needs to be redirected as we aren't getting that particular object
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

            // Is this object a ROT or MASTER, if it is then we do not perform any changes to re-binding
            if (this.IsRecordOfTruth(e.Data))
            {
                // Record of truth, ensure we update the appropirate
                if (e.Data is Entity entityData)
                {
                    // Get the MDM relationship as this record will point > MDM
                    var masterRelationship = entityData.Relationships.First(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);

                    // Establish a master
                    var bundle = sender as Bundle ?? new Bundle();
                    if (!bundle.Item.Contains(e.Data))
                        bundle.Add(e.Data);

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


                // Perform matching
                var bundle = this.PerformMdmMatch(e.Data);
                e.Cancel = true;

                // Is the caller the bundle MDM? if so just add 
                if (sender is Bundle)
                {
                    (sender as Bundle).Item.Remove(e.Data);
                    (sender as Bundle).Item.AddRange(bundle.Item);
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

        /// <summary>
        /// Determine whether the specified <paramref name="data"/> represents a ROT
        /// </summary>
        private bool IsRecordOfTruth(T data)
        {
            if (data is Entity entityData)
            {
                return entityData.Tags.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true||
                    entityData.DeterminerConceptKey == MdmConstants.RecordOfTruthDeterminer ||
                    ApplicationServiceContext.Current.GetService<IDataPersistenceService<EntityRelationship>>().Count(o => o.TargetEntityKey == data.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal) > 0;
            }
            else if (data is Act actData)
            {
                this.m_traceSource.TraceUntestedWarning();
                return actData.Tags.Any(o => o.TagKey == "$mdm.type" && o.Value == "T") == true ||
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

            // Only the server is allowed to establish master records , clients are not permitted
            if (ApplicationServiceContext.Current.HostType == SanteDBHostType.Client ||
                ApplicationServiceContext.Current.HostType == SanteDBHostType.Gateway)
                return;

            if (!e.Data.Key.HasValue)
                e.Data.Key = Guid.NewGuid(); // Assign a key if one is not set
                                             // Is this object a ROT or MASTER, if it is then we do not perform any changes to re-binding
            if (this.IsRecordOfTruth(e.Data))
            {
                // Record of truth, ensure we update the appropirate
                if (e.Data is Entity entityData)
                {
                    // Get the MDM relationship as this record will point > MDM
                    var masterRelationship = entityData.Relationships.First(o => o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);

                    // Establish a master
                    var bundle = sender as Bundle ?? new Bundle();
                    if (!bundle.Item.Contains(e.Data))
                        bundle.Add(e.Data);

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

                // Is the caller the bundle MDM? if so just add 
                if (sender is Bundle)
                {
                    //(sender as Bundle).Item.Remove(e.Data);
                    (sender as Bundle).Item.AddRange(bundle.Item.Where(o => o != e.Data));
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

        /// <summary>
        /// Perform an MDM match process to link the probable and definitive match
        /// </summary>
        private Bundle PerformMdmMatch(T entity)
        {
            this.m_traceSource.TraceVerbose("{0} : MDM will perform candidate match for entity", entity);

            var matchService = ApplicationServiceContext.Current.GetService<IRecordMatchingService>();
            if (matchService == null)
                throw new InvalidOperationException("Cannot operate MDM mode without matching service"); // Cannot make determination of matching

            var taggable = (ITaggable)entity;
            var relationshipType = entity is Entity ? typeof(EntityRelationship) : typeof(ActRelationship);
            var relationshipService = ApplicationServiceContext.Current.GetService(typeof(IDataPersistenceService<>).MakeGenericType(relationshipType)) as IDataPersistenceService;

            // Create generic method for call with proper arguments
            var matchMethod = typeof(IRecordMatchingService).GetGenericMethod(nameof(IRecordMatchingService.Match), new Type[] { entity.GetType() }, new Type[] { entity.GetType(), typeof(String) });
            if (matchMethod == null)
                throw new InvalidOperationException("State is invalid - Could not find matching service method - Does it implement IRecordMatchingService properly?");

            var existingMasterKey = this.GetMaster(entity);
            this.m_traceSource.TraceVerbose("{0} : Entity has existing master record with ID {1]", entity, existingMasterKey);

            var rawMatches = matchMethod.Invoke(matchService, new object[] { entity, this.m_resourceConfiguration.MatchConfiguration }) as IEnumerable;
            var matchingRecords = rawMatches.OfType<IRecordMatchResult>();

            this.m_traceSource.TraceVerbose("{0} : Matching layer has identified {1} candidate(s)", entity, matchingRecords.Count());

            // Matching records can only match with those that have MASTER records
            var matchGroups = matchingRecords
                .Where(o => o.Record.Key != entity.Key)
                .Select(o => new MasterMatch(this.GetMaster(o.Record).Value, o))
                .Distinct(new MasterMatchEqualityComparer())
                .GroupBy(o => o.MatchResult.Classification)
                .ToDictionary(o => o.Key, o => o.Select(g => g.Master).Distinct());

            if (!matchGroups.ContainsKey(RecordMatchClassification.Match))
                matchGroups.Add(RecordMatchClassification.Match, new List<Guid>());
            if (!matchGroups.ContainsKey(RecordMatchClassification.Probable))
                matchGroups.Add(RecordMatchClassification.Probable, new List<Guid>());

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
            var ignoreList = taggable.Tags.FirstOrDefault(o => o.TagKey == "mdm.ignore")?.Value.Split(';').AsEnumerable() ?? new string[0];

            // Existing probable links
            var existingProbableQuery = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={entity.Key}&relationshipType={MdmConstants.CandidateLocalRelationship}") }) as Expression;
            int tr = 0;
            var existingProbableLinks = relationshipService.Query(existingProbableQuery, 0, 100, out tr);

            // We want to obsolete any existing links that are no longer valid
            foreach (var er in existingProbableLinks.OfType<EntityRelationship>().Where(er => matchGroups[RecordMatchClassification.Match]?.Any(m => m == er.TargetEntityKey) == false && matchGroups[RecordMatchClassification.Probable]?.Any(m => m == er.TargetEntityKey) == false))
            {
                er.ObsoleteVersionSequenceId = Int32.MaxValue;
                insertData.Add(er);
            }
            foreach (var ar in existingProbableLinks.OfType<ActRelationship>().Where(ar => matchGroups[RecordMatchClassification.Match]?.Any(m => m == ar.TargetActKey) == false && matchGroups[RecordMatchClassification.Probable]?.Any(m => m == ar.TargetActKey) == false))
            {
                ar.ObsoleteVersionSequenceId = Int32.MaxValue;
                insertData.Add(ar);
            }

            // There is exactly one match and it is set to automerge
            if (matchGroups.ContainsKey(RecordMatchClassification.Match) && matchGroups[RecordMatchClassification.Match]?.Count() == 1
                && this.m_resourceConfiguration.AutoMerge)
            {
                // Next, ensure that the new master is set
                this.m_traceSource.TraceVerbose("{0}: Entity has exactly 1 exact match and the configuration indicates auto-merge", entity);
                var master = matchGroups[RecordMatchClassification.Match].Single();

                if (master != existingMasterKey)
                { 
                    // change in exact match to another master
                    // We want to remove all previous master matches
                    var query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                        .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;

                    var rels = relationshipService.Query(query, 0, 100, out tr);

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
                        insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.OriginalMasterRelationship, entity, oldMasterId));

                        query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMasterId}&source=!{entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        relationshipService.Query(query, 0, 0, out tr);
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

                    insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity, master));
                }
                // dataService.Update(master);
                // No change in master
            }
            else
            {
                // We want to create a new master for this record?
                var query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                    .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                var rels = relationshipService.Query(query, 0, 100, out tr).OfType<Object>();

                if (!existingMasterKey.HasValue) // There is no master
                {
                    this.m_traceSource.TraceVerbose("{0}: Entity has no existing master record. Creating one.", entity);
                    var master = this.CreateMasterRecord();
                    if (master is Entity masterEntity && entity is Entity localEntity)
                        masterEntity.DeterminerConceptKey = localEntity.DeterminerConceptKey;
                    else if (master is Act masterAct && entity is Act localAct)
                        masterAct.MoodConceptKey = localAct.MoodConceptKey;

                    insertData.Add(master as IdentifiedData);
                    insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity, master.Key));
                }
                else if(!matchGroups[RecordMatchClassification.Match].All(o=>o == existingMasterKey)) // No match with the existing master => Redirect the master
                {
                    // Is this the only record in the current master relationship?
                    var oldMasterRel = rels.OfType<IdentifiedData>().SingleOrDefault()?.Clone();
                    if (oldMasterRel != null) // IS the master rel even in the db?
                    {
                        var oldMasterId = (Guid)oldMasterRel.GetType().GetQueryProperty("target").GetValue(oldMasterRel);
                        ApplicationServiceContext.Current.GetService<IDataCachingService>()?.Remove(oldMasterRel.Key.Value);

                        // Query for other masters
                        query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMasterId}&source=!{entity.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        relationshipService.Query(query, 0, 0, out tr);
                        if (tr > 0) // Old master has other records, we want to obsolete our current reference to it and then establish a new master
                        {
                            var master = this.CreateMasterRecord();
                            if (master is Entity masterEntity && entity is Entity localEntity)
                                masterEntity.DeterminerConceptKey = localEntity.DeterminerConceptKey;
                            else if (master is Act masterAct && entity is Act localAct)
                                masterAct.MoodConceptKey = localAct.MoodConceptKey;

                            insertData.Add(master as IdentifiedData);
                            insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, entity, master.Key));
                            if (oldMasterRel is EntityRelationship)
                                (oldMasterRel as EntityRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;
                            else
                                (oldMasterRel as ActRelationship).ObsoleteVersionSequenceId = Int32.MaxValue;
                            insertData.Insert(0, oldMasterRel);
                            insertData.Add(this.CreateRelationship(relationshipType, MdmConstants.OriginalMasterRelationship, entity, oldMasterId));
                            this.m_traceSource.TraceVerbose("{0}: Old master record still hase other locals, creating new master {1} and detaching old relationship {2}", entity, master, oldMasterRel);
                        }
                        // If not we want to keep our link to the current master
                        else
                        {
                            insertData.Add(oldMasterRel);
                            this.m_traceSource.TraceVerbose("{0}: Entity was identified as not matching its current MASTER however the old master only has one local so we'll keep it", entity);
                        }
                    }
                }
                else if(rels.Any()) // no change in master so just reuse rels from DB
                    insertData.Add(rels.OfType<IdentifiedData>().SingleOrDefault());


                var nonMasterMatches = matchGroups[RecordMatchClassification.Match]?.Where(o => o != existingMasterKey);

                // Direct matches become candidate records
                if (nonMasterMatches.Any())
                {
                    // Get existing candidate locals (we don't want to report twice)
                    query = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                        .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={entity.Key}&relationshipType={MdmConstants.CandidateLocalRelationship}") }) as Expression;
                    rels = relationshipService.Query(query, 0, 100, out tr).OfType<Object>();

                    // Add any NEW match data which we didn't know about before
                    insertData.AddRange(nonMasterMatches
                        .Where(m => !ignoreList.Contains(m.ToString()))
                        .Select(m => this.CreateRelationship(relationshipType, MdmConstants.CandidateLocalRelationship, entity, m)));

                    // Remove all rels which don't appear in the insert data
                    foreach (var r in rels.OfType<IdentifiedData>())
                        if (!nonMasterMatches.Any(a => a == (r as EntityRelationship)?.TargetEntityKey || a == (r as ActRelationship)?.TargetActKey))
                            (r as IVersionedAssociation).ObsoleteVersionSequenceId = Int32.MaxValue;
                        else
                            insertData.Add(r);
                }
            }

            // Add probable records
            if (matchGroups[RecordMatchClassification.Probable] != null)
                insertData.AddRange(matchGroups[RecordMatchClassification.Probable]
                    .Where(m => !ignoreList.Contains(m.ToString())) // ignore list
                    .Select(m => this.CreateRelationship(relationshipType, MdmConstants.CandidateLocalRelationship, entity, m)));


            // Now we want to make sure the relationships on the entity (provided) don't contain any tainted relationship data as we'll be returning this in the bundle
            if (entity is Entity entityData) entityData.Relationships.RemoveAll(o => insertData.OfType<EntityRelationship>().Any(i => i.RelationshipTypeKey == o.RelationshipTypeKey));
            else if (entity is Act actData) actData.Relationships.RemoveAll(o => insertData.OfType<ActRelationship>().Any(i => i.RelationshipTypeKey == o.RelationshipTypeKey));

            this.m_traceSource.TraceVerbose("{0}: MDM matching has identified {1} changes to be made to the accomodate new data", entity, insertData.Count);
            return new Bundle() { Item = insertData };
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
        /// <param name="relationshipClassification"></param>
        /// <param name="sourceEntity"></param>
        /// <param name="targetEntity"></param>
        /// <returns></returns>
        private IdentifiedData CreateRelationship(Type relationshipType, Guid relationshipClassification, T sourceEntity, Guid? targetEntity)
        {
            var relationship = Activator.CreateInstance(relationshipType, relationshipClassification, targetEntity) as IdentifiedData;
            relationship.Key = Guid.NewGuid();
            (relationship as ISimpleAssociation).SourceEntityKey = sourceEntity.Key;
            (relationship as ISimpleAssociation).SourceEntity = sourceEntity;
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
        public virtual T Merge(T master, IEnumerable<T> linkedDuplicates)
        {

            DataMergingEventArgs<T> preEventArgs = new DataMergingEventArgs<T>(master, linkedDuplicates);
            this.Merging?.Invoke(this, preEventArgs);
            if (preEventArgs.Cancel)
            {
                this.m_traceSource.TraceInfo("Pre-event handler has indicated a cancel of merge on {0}", master);
                return null;
            }
            master = preEventArgs.Master; // Allow resource to update these fields
            linkedDuplicates = preEventArgs.Linked;

            // Relationship type
            var relationshipType = master is Entity ? typeof(EntityRelationship) : typeof(ActRelationship);
            var relationshipService = ApplicationServiceContext.Current.GetService(typeof(IDataPersistenceService<>).MakeGenericType(relationshipType)) as IDataPersistenceService;

            // Ensure that MASTER is in fact a master
            IDataPersistenceService masterService = master is Entity ? ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>() as IDataPersistenceService : ApplicationServiceContext.Current.GetService<IDataPersistenceService<Act>>() as IDataPersistenceService;
            var masterData = masterService.Get(master.Key.Value) as IClassifiable;
            if (masterData.ClassConceptKey == MdmConstants.MasterRecordClassification && !AuthenticationContext.Current.Principal.IsInRole("SYSTEM"))
                ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.WriteMdmMaster);
            else
            {
                this.EnsureProvenance(master, AuthenticationContext.Current.Principal);
                var existingMasterQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                           .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={master.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                int tr = 0;
                var masterRel = relationshipService.Query(existingMasterQry, 0, 2, out tr).OfType<ISimpleAssociation>().SingleOrDefault();
                masterData = masterService.Get((Guid)masterRel.GetType().GetQueryProperty("target").GetValue(masterRel)) as IClassifiable;
            }

            // For each of the linked duplicates we want to get the master relationships 
            foreach (var ldpl in linkedDuplicates)
            {
                // Is the linked duplicate a master record?
                var linkedClass = masterService.Get(ldpl.Key.Value) as IClassifiable;
                if (linkedClass.ClassConceptKey == MdmConstants.MasterRecordClassification && !AuthenticationContext.Current.Principal.IsInRole("SYSTEM"))
                    ApplicationServiceContext.Current.GetService<IPolicyEnforcementService>().Demand(MdmPermissionPolicyIdentifiers.MergeMdmMaster);
                else
                    this.EnsureProvenance(ldpl, AuthenticationContext.Current.Principal);

                // Allowed merges
                // LOCAL > MASTER - A local record is being merged into a MASTER
                // MASTER > MASTER - Two MASTER records are being merged (administrative merge)
                // LOCAL > LOCAL - Two LOCAL records are being merged
                if (linkedClass.ClassConceptKey == masterData.ClassConceptKey)
                {
                    if (linkedClass.ClassConceptKey == MdmConstants.MasterRecordClassification) // MASTER <> MASTER
                    {
                        // First, we move all references from the subsumed MASTER to the new MASTER
                        var existingMasterQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={ldpl.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        int tr = 0;
                        var existingMasters = relationshipService.Query(existingMasterQry, 0, 100, out tr);
                        foreach (var erel in existingMasters)
                        {
                            erel.GetType().GetQueryProperty("target").SetValue(erel, master.Key);
                            relationshipService.Update(erel);
                        }

                        // Now we want to mark LMASTER replaced by MASTER
                        var mrel = this.CreateRelationship(relationshipType, EntityRelationshipTypeKeys.Replaces, master, ldpl.Key);
                        relationshipService.Insert(mrel);
                        ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>().Obsolete(ldpl, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);
                    }
                    else // LOCAL <> LOCAL
                    {
                        // With local to local we want to remove the existing MASTER from the replaced local and redirect it to the MASTER of the new local
                        var existingMasterQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={ldpl.Key}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        int tr = 0;
                        var localMaster = relationshipService.Query(existingMasterQry, 0, 2, out tr).OfType<ISimpleAssociation>().SingleOrDefault();

                        Guid oldMaster = (Guid)localMaster.GetType().GetQueryProperty("target").GetValue(localMaster);
                        // Now we want to move the local master to the master of the MASTER LOCAL
                        localMaster.GetType().GetQueryProperty("target").SetValue(localMaster, (masterData as IdentifiedData).Key);
                        relationshipService.Update(localMaster);

                        // Now we want to set replaces relationship
                        var mrel = this.CreateRelationship(relationshipType, EntityRelationshipTypeKeys.Replaces, master, ldpl.Key);
                        relationshipService.Insert(mrel);
                        ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>().Obsolete(ldpl, TransactionMode.Commit, AuthenticationContext.SystemPrincipal);

                        // Check if the master is orphaned, if so obsolete it
                        existingMasterQry = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMaster}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                        var otherMaster = relationshipService.Query(existingMasterQry, 0, 0, out tr);
                        if (tr == 0)
                            masterService.Obsolete(masterService.Get(oldMaster));
                    }
                }
                // LOCAL > MASTER
                else if (masterData.ClassConceptKey == MdmConstants.MasterRecordClassification &&
                    linkedClass.ClassConceptKey != MdmConstants.MasterRecordClassification)
                {
                    // LOCAL to MASTER is merged as removing all probables and assigning the MASTER relationship from the 
                    // existing master to the identified master
                    var existingQuery = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                           .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"source={ldpl.Key}&relationshipType={MdmConstants.MasterRecordRelationship}&relationshipType={MdmConstants.CandidateLocalRelationship}") }) as Expression;
                    int tr = 0;
                    var existingRelationships = relationshipService.Query(existingQuery, 0, null, out tr);
                    var oldMaster = Guid.Empty;
                    // Remove existing relationships
                    foreach (var rel in existingRelationships)
                    {
                        if (rel.GetType().GetQueryProperty("relationshipType").GetValue(rel).Equals(MdmConstants.MasterRecordClassification))
                            oldMaster = (Guid)rel.GetType().GetQueryProperty("target").GetValue(rel);
                        relationshipService.Obsolete(rel);
                    }

                    // Add relationship 
                    relationshipService.Insert(this.CreateRelationship(relationshipType, MdmConstants.MasterRecordRelationship, ldpl, master.Key));

                    // Obsolete the old master
                    existingQuery = typeof(QueryExpressionParser).GetGenericMethod(nameof(QueryExpressionParser.BuildLinqExpression), new Type[] { relationshipType }, new Type[] { typeof(NameValueCollection) })
                            .Invoke(null, new object[] { NameValueCollection.ParseQueryString($"target={oldMaster}&relationshipType={MdmConstants.MasterRecordRelationship}") }) as Expression;
                    var otherMaster = relationshipService.Query(existingQuery, 0, 0, out tr);
                    if (tr == 0)
                        masterService.Obsolete(masterService.Get(oldMaster));
                }
                else
                    throw new InvalidOperationException("Invalid merge. Only LOCAL>MASTER, MASTER>MASTER or LOCAL>LOCAL are supported");
            }

            T retVal = default(T);
            if (masterData.ClassConceptKey == MdmConstants.MasterRecordClassification)
                retVal = masterData is Entity ? new EntityMaster<T>((Entity)masterService.Get(master.Key.Value)).GetMaster(AuthenticationContext.Current.Principal) :
                    new ActMaster<T>((Act)masterService.Get(master.Key.Value)).GetMaster(AuthenticationContext.Current.Principal);
            else
                retVal = (T)masterService.Get(master.Key.Value);

            this.Merged?.Invoke(this, new DataMergeEventArgs<T>(retVal, linkedDuplicates));
            return retVal;
        }

        /// <summary>
        /// Ensures that <paramref name="master"/> is owned by application granted by <paramref name="principal"/>
        /// </summary>
        /// <param name="master"></param>
        /// <param name="principal"></param>
        private void EnsureProvenance(T master, IPrincipal principal)
        {
            var provenance = (master as BaseEntityData)?.LoadProperty<SecurityProvenance>("CreatedBy");
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
        /// <param name="master"></param>
        /// <param name="unmergeDuplicate"></param>
        /// <returns></returns>
        public virtual T Unmerge(T master, T unmergeDuplicate)
        {
            throw new NotImplementedException();
        }
    }
}