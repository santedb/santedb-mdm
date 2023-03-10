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
using SanteDB.Core;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Data;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security;
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
    /// A non-generic data manager
    /// </summary>
    public abstract class MdmDataManager : IDataManagedLinkProvider
    {
        /// <summary>
        /// Determine if the object is a master
        /// </summary>
        public abstract bool IsMaster(Guid dataKey);
        
        /// <summary>
        /// Determine if the object is a local
        /// </summary>
        public abstract bool IsLocal(Guid dataKey);

        /// <summary>
        /// Determine if <paramref name="principal"/> is the owner of <paramref name="localKey"/>
        /// </summary>
        public abstract bool IsOwner(Guid localKey, IPrincipal principal);

        /// <summary>
        /// Get the local for <paramref name="masterKey"/> owned by <paramref name="principal"/>
        /// </summary>
        public abstract IdentifiedData GetLocalFor(Guid masterKey, IPrincipal principal);

        /// <summary>
        /// Create a local for <paramref name="masterRecord"/> owned by <paramref name="principal"/>
        /// </summary>
        public abstract IdentifiedData CreateLocalFor(IdentifiedData masterRecord);

        /// <summary>
        /// Refactor relationships
        /// </summary>
        public abstract void RefactorRelationships(IEnumerable<IdentifiedData> item, Guid fromEntityKey, Guid toEntityKey);

        /// <summary>
        /// Get all MDM candidate locals regardless of where they are attached
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetAllMdmCandidateLocals();

        /// <summary>
        /// Gets all local associations between <paramref name="masterKey"/> and its master
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetAssociatedLocals(Guid masterKey);

        /// <summary>
        /// Get all candidate associations between <paramref name="masterKey"/>
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetCandidateLocals(Guid masterKey);

        /// <summary>
        /// Get ignore associations
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetIgnoredCandidateLocals(Guid masterKey);

        /// <summary>
        /// Get all associations for which this is a candidate to another master
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetEstablishedCandidateMasters(Guid localKey);

        /// <summary>
        /// Get ignore associations
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetIgnoredMasters(Guid localKey);

        /// <summary>
        /// Get the master entity
        /// </summary>
        public abstract IMdmMaster MdmGet(Guid masterKey);

        /// <summary>
        /// Get a MDM Master for the specified local key
        /// </summary>
        public abstract IMdmMaster GetMasterFor(Guid masterOrLocalKey);

        /// <summary>
        /// Get a MDM Master for the specified local key
        /// </summary>
        public abstract IMdmMaster CreateMasterContainerForMasterEntity(IAnnotatedResource masterObject);

        /// <summary>
        /// Merge two master records together
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMergeMasters(Guid survivorKey, Guid victimKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions to ignore candidate matches
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Un-ignore a candidiate match
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxUnIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, List<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions to establish MDM master link
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMasterLink(Guid fromKey, Guid toKey, IEnumerable<IdentifiedData> context, bool verified);

        /// <summary>
        /// Create transaction instructions to unlink a master
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMasterUnlink(Guid fromKey, Guid toKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Given a LOCAL match MASTER records that might be candidates
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMatchMasters(IdentifiedData local, List<IdentifiedData> context);

        /// <summary>
        /// Given a MASTER detect LOCALS which might be candidates
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxDetectCandidates(IdentifiedData master, List<IdentifiedData> context);

        /// <summary>
        /// Get all MDM associations for this local
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetAllMdmAssociations(Guid localKey);

        /// <summary>
        /// Get the master record for the specified local record
        /// </summary>
        public ITargetedAssociation GetMasterRelationshipFor(Guid localKey) =>
            this.GetAllMdmAssociations(localKey).FirstOrDefault(o => o.AssociationTypeKey == MdmConstants.MasterRecordRelationship);


        /// <summary>
        /// Fired when a managed link is established
        /// </summary>
        public event EventHandler<DataManagementLinkEventArgs> ManagedLinkEstablished;

        /// <summary>
        /// Fired when a managed link is removed
        /// </summary>
        public event EventHandler<DataManagementLinkEventArgs> ManagedLinkRemoved;


        /// <summary>
        /// Fires the <see cref="ManagedLinkEstablished"/> event
        /// </summary>
        internal void FireManagedLinkEstablished(ITargetedAssociation establishedLink)
        {
            this.ManagedLinkEstablished?.Invoke(this, new DataManagementLinkEventArgs(establishedLink));
        }

        /// <summary>
        /// Fires the <see cref="ManagedLinkRemoved"/> event
        /// </summary>
        internal void FireManagedLinkRemoved(ITargetedAssociation establishedLink)
        {
            this.ManagedLinkRemoved?.Invoke(this, new DataManagementLinkEventArgs(establishedLink));
        }

        /// <inheritdoc/>
        public abstract IdentifiedData ResolveManagedRecord(IdentifiedData forSource);
        /// <inheritdoc/>
        public abstract IdentifiedData ResolveOwnedRecord(IdentifiedData forTarget, IPrincipal ownerPrincipal);
    }

    /// <summary>
    /// Represents a data manager which actually interacts with the underlying repository
    /// </summary>
    public abstract class MdmDataManager<TModel> : MdmDataManager, IDataManagedLinkProvider<TModel>
        where TModel : IdentifiedData, IHasTypeConcept, IHasClassConcept, IHasRelationships
    {

        // Entity type maps 
        private static readonly Dictionary<Guid, Type> m_entityTypeMap = new Dictionary<Guid, Type>() {
            { EntityClassKeys.Patient, typeof(Patient) },
            { EntityClassKeys.Provider, typeof(Provider) },
            { EntityClassKeys.Organization, typeof(Organization) },
            { EntityClassKeys.Place, typeof(Place) },
            { EntityClassKeys.CityOrTown, typeof(Place) },
            { EntityClassKeys.Country, typeof(Place) },
            { EntityClassKeys.CountyOrParish, typeof(Place) },
            { EntityClassKeys.StateOrProvince, typeof(Place) },
            { EntityClassKeys.PrecinctOrBorough, typeof(Place) },
            { EntityClassKeys.ServiceDeliveryLocation, typeof(Place) },
            { EntityClassKeys.Person, typeof(Person) },
            { EntityClassKeys.ManufacturedMaterial, typeof(ManufacturedMaterial) },
            { EntityClassKeys.Material, typeof(Material) }
        };

        /// <summary>
        /// Persistence service
        /// </summary>
        protected IDataPersistenceService m_underlyingTypePersistence;

        // Ad-hoc cache
        protected readonly IAdhocCacheService m_adhocCache;

        /// <summary>
        /// Create a new manager base
        /// </summary>
        internal MdmDataManager(IDataPersistenceService underlyingDataPersistence)
        {
            if (underlyingDataPersistence == null)
            {
                throw new InvalidOperationException($"Type {typeof(TModel).FullName} does not have a persistence service registered");
            }

            this.m_underlyingTypePersistence = underlyingDataPersistence;
            this.m_adhocCache = ApplicationServiceContext.Current.GetService<IAdhocCacheService>();
        }

        /// <summary>
        /// Determine if the record is a ROT
        /// </summary>
        public abstract bool IsRecordOfTruth(TModel data);

        /// <summary>
        /// Determine if the object is a master
        /// </summary>
        public abstract bool IsMaster(TModel data);

        /// <summary>
        /// Create local for the specified principal
        /// </summary>
        public abstract TModel CreateLocalFor(TModel masterRecord);

        /// <summary>
        /// Converts the <paramref name="local"/> to a ROT local
        /// </summary>
        public abstract TModel PromoteRecordOfTruth(TModel local);

        /// <summary>
        /// Get the master for the specified local
        /// </summary>
        public abstract IdentifiedData GetMasterFor(TModel local);

        /// <summary>
        /// Extracts the transactional components which might be in <paramref name="store"/> and return them
        /// </summary>
        public abstract IEnumerable<ISimpleAssociation> ExtractRelationships(TModel store);

        /// <summary>
        /// Validate the MDM state
        /// </summary>
        public abstract IEnumerable<DetectedIssue> ValidateMdmState(TModel data);

        /// <summary>
        /// Synthesize the query
        /// </summary>
        public abstract IQueryResultSet<TModel> MdmQuery(NameValueCollection query, NameValueCollection localQuery, IPrincipal asPrincipal);

        /// <summary>
        /// Gets the raw object from the underlying persistence service identified by the key (whether it is MASTER ENTITY or LOCAL or SYNTH)
        /// </summary>
        public object GetRaw(Guid key)
        {
            return this.m_underlyingTypePersistence.Get(key);
        }

        /// <summary>
        /// Ensures that <paramref name="principal"/> has access to a local <paramref name="data"/>
        /// </summary>
        public abstract bool IsOwner(TModel data, IPrincipal principal);

        /// <summary>
        /// Perform an MDM obsolete operation
        /// </summary>
        /// <returns>Whether the obsolete operation resulted in cascading obsolete to the master</returns>
        public abstract IEnumerable<IdentifiedData> MdmTxObsolete(TModel data, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create a transaction instructions which update <paramref name="data"/> to be the ROT
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxSaveRecordOfTruth(TModel data, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions which update the <paramref name="data"/> as a local
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxSaveLocal(TModel data, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions which update <paramref name="local"/> to match all masters
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMatchMasters(TModel local, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create a new master for the local
        /// </summary>
        public abstract IdentifiedData EstablishMasterFor(TModel local);

        /// <summary>
        /// Match masters
        /// </summary>
        public override IEnumerable<IdentifiedData> MdmTxMatchMasters(IdentifiedData data, List<IdentifiedData> context) =>
            this.MdmTxMatchMasters((TModel)data, context);


        /// <summary>
        /// Get all managed reference links that are established
        /// </summary>
        public IEnumerable<ITargetedAssociation> FilterManagedReferenceLinks(IEnumerable<ITargetedAssociation> forRelationships) => forRelationships.Where(o => o.AssociationTypeKey == MdmConstants.MasterRecordRelationship);

        /// <summary>
        /// Add a managed reference link
        /// </summary>
        public ITargetedAssociation AddManagedReferenceLink(TModel sourceObject, TModel targetObject)
        {
            ITargetedAssociation retVal = null;
            if (sourceObject is Entity e)
            {
                retVal = new EntityRelationship(MdmConstants.MasterRecordRelationship, e);
            }
            else if (sourceObject is Act a)
            {
                retVal = new ActRelationship(MdmConstants.MasterRecordRelationship, a);
            }
            sourceObject.AddRelationship(retVal);
            return retVal;
        }

        /// <summary>
        /// Get master for <paramref name="forSource"/> or, if it is already a master or not MDM controlled return <paramref name="forSource"/>
        /// </summary>
        public TModel ResolveOwnedRecord(TModel forSource, IPrincipal ownerPrincipal)
        {
            IdentifiedData master = forSource;
            if(forSource.ClassConceptKey != MdmConstants.MasterRecordClassification) 
            {
                master = this.GetMasterRelationshipFor(forSource.Key.Value).LoadProperty(o=>o.TargetEntity) as IdentifiedData;
                if (master == null)
                {
                    return null;
                }
            }
            return (TModel)this.GetLocalFor(master.Key.Value, ownerPrincipal);
        }

        /// <inheritdoc/>
        public TModel ResolveManagedRecord(TModel forSource)
        {
            if (forSource.ClassConceptKey == MdmConstants.MasterRecordClassification)
            {
                if (m_entityTypeMap.TryGetValue(forSource.TypeConceptKey.Value, out var mapType) && typeof(TModel) == mapType) // We are the correct handler for this
                {
                    return this.CreateMasterContainerForMasterEntity(forSource).Synthesize(AuthenticationContext.Current.Principal) as TModel;
                }
                else if (MdmDataManagerFactory.TryGetDataManager(mapType, out var manager))// we are not
                {
                    return manager.CreateMasterContainerForMasterEntity(forSource).Synthesize(AuthenticationContext.Current.Principal) as TModel;
                }
                else
                {
                    return forSource;
                }
            }
            else
                return forSource;
        }

        /// <summary>
        /// For any relationship where the local points to a MASTER which is not appropriate for MDM - remove 
        /// </summary>
        /// <param name="local">The local record which may be pointing to masters</param>
        /// <param name="principal">The principal under which a local should exist</param>
        /// <param name="context">If the request is part of an existing bundle the bundle</param>
        public virtual void RepointRelationshipsToLocals(TModel local, IPrincipal principal, List<IdentifiedData> context)
        {
            foreach (var rel in local.Relationships.Where(o => !MdmConstants.MDM_RELATIONSHIP_TYPES.Contains(o.AssociationTypeKey.GetValueOrDefault())))
            {
                // TODO: Cross local pointers should not be permitted either - 
                if(!rel.TargetEntityKey.HasValue) // no target?
                {
                    continue;
                }
                else if (this.IsMaster(rel.TargetEntityKey.Value))
                {
                    var master = this.m_underlyingTypePersistence.Get(rel.TargetEntityKey.Value) as IHasTypeConcept;
                    if (m_entityTypeMap.TryGetValue(master.TypeConceptKey.Value, out var managedType) &&
                        MdmDataManagerFactory.TryGetDataManager(managedType, out var manager))
                    {
                        var localForTarget = manager.GetLocalFor(rel.TargetEntityKey.Value, principal);
                        if (localForTarget == null)
                        {
                            localForTarget = manager.CreateLocalFor(master as IdentifiedData);
                            if(context != null)
                            {
                                context.Add(localForTarget);
                                rel.TargetEntityKey = localForTarget.Key;
                            }
                            else
                            {
                                rel.TargetEntity = localForTarget;
                            }
                        }
                        else
                        {
                            rel.TargetEntityKey = localForTarget.Key;
                        }
                    }
                }
                else if(this.IsLocal(rel.TargetEntityKey.Value) && !this.IsOwner(rel.TargetEntityKey.Value, principal))
                {
                    // This should not happen - basically someone is trying to link directly to a local on another record 
                    // We'll try to get an appropriate local
                    var myLocal = this.GetLocalFor(this.GetMasterRelationshipFor(rel.TargetEntityKey.Value).TargetEntityKey.Value, principal);
                    if(myLocal == null)
                    {
                        throw new InvalidOperationException("MDM does not permit local record to directly reference other locals not owned by the creator");
                    }
                    else
                    {
                        rel.TargetEntityKey = myLocal.Key;
                    }
                }
            }
        }

        /// <inheritdoc/>
        public override IdentifiedData ResolveManagedRecord(IdentifiedData forSource) => this.ResolveManagedRecord((TModel)forSource);

        /// <inheritdoc/>
        public override IdentifiedData ResolveOwnedRecord(IdentifiedData forTarget, IPrincipal ownerPrincipal) => this.ResolveOwnedRecord((TModel)forTarget, ownerPrincipal);

    }
}