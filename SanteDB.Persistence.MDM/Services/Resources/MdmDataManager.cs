﻿using SanteDB.Core;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Configuration;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Claims;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a data manager which actually interacts with the underlying repository
    /// </summary>
    public abstract class MdmDataManager<TModel>
        where TModel : IdentifiedData, new()
    {

        /// <summary>
        /// Persistence service
        /// </summary>
        protected IDataPersistenceService m_underlyingTypePersistence;

        /// <summary>
        /// Create a new manager base
        /// </summary>
        internal MdmDataManager(IDataPersistenceService underlyingDataPersistence)
        {
            this.m_underlyingTypePersistence = underlyingDataPersistence;
        }

        /// <summary>
        /// Gets or creates a writable target object for the specified principal
        /// </summary>
        public abstract TModel GetLocalFor(TModel data, IPrincipal principal);

        /// <summary>
        /// Determine if the object is a master
        /// </summary>
        public abstract bool IsMaster(Guid dataKey);

        /// <summary>
        /// Determine if the object is a master
        /// </summary>
        public abstract bool IsMaster(TModel data);

        /// <summary>
        /// Create local for the specified principal
        /// </summary>
        public abstract TModel CreateLocalFor(TModel masterRecord);

        /// <summary>
        /// Determine if the record is a ROT
        /// </summary>
        public abstract bool IsRecordOfTruth(TModel data);

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
        /// Refactor relationships
        /// </summary>
        public abstract void RefactorRelationships(List<IdentifiedData> item, Guid fromEntityKey, Guid toEntityKey);

        /// <summary>
        /// Validate the MDM state
        /// </summary>
        public abstract IEnumerable<DetectedIssue> ValidateMdmState(TModel data);

        /// <summary>
        /// Synthesize the query 
        /// </summary>
        public abstract IEnumerable<IMdmMaster> MdmQuery(NameValueCollection query, NameValueCollection localQuery, Guid? queryId, int offset, int? count, out int totalResults);

        /// <summary>
        /// Gets the raw object from the underlying persistence service identified by the key (whether it is MASTER ENTITY or LOCAL or SYNTH)
        /// </summary>
        public object GetRaw(Guid key)
        {
            return this.m_underlyingTypePersistence.Get(key);
        }

        /// <summary>
        /// Get the master entity
        /// </summary>
        public abstract IMdmMaster MdmGet(Guid masterKey);

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
        /// Match MDM masters
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmMatchMasters(TModel local, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create a new master for the local
        /// </summary>
        public abstract IdentifiedData EstablishMasterFor(TModel local);

        /// <summary>
        /// Create the specified data manager
        /// </summary>
        public static MdmDataManager<TModel> Create(ResourceMergeConfiguration configuration)
        {
            if(configuration.MatchConfiguration == null || !configuration.MatchConfiguration.Any())
            {
                configuration.MatchConfiguration = new List<ResourceMergeMatchConfiguration>()
                {
                    new ResourceMergeMatchConfiguration(MdmConstants.MdmIdentityMatchConfiguration, true)
                };
            }
            if (typeof(Entity).IsAssignableFrom(typeof(TModel)))
                return (MdmDataManager<TModel>)Activator.CreateInstance(typeof(MdmEntityDataManager<>).MakeGenericType(configuration.ResourceType), configuration);
            throw new InvalidOperationException("Cannot create MDM data listener for this type");
        }
    }
}