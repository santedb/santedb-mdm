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
using System.Linq.Expressions;
using System.Security.Principal;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Represents a data manager which actually interacts with the underlying repository
    /// </summary>
    public abstract class MdmDataManager<TModel>
        where TModel : IdentifiedData
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
            if (underlyingDataPersistence == null)
                throw new InvalidOperationException($"Type {typeof(TModel).FullName} does not have a persistence service registered");
            this.m_underlyingTypePersistence = underlyingDataPersistence;
        }

        /// <summary>
        /// Gets or creates a writable target object for the specified principal
        /// </summary>
        public abstract TModel GetLocalFor(Guid dataKey, IPrincipal principal);


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
        public abstract IdentifiedData GetRawMaster(TModel local);

        /// <summary>
        /// Extracts the transactional components which might be in <paramref name="store"/> and return them
        /// </summary>
        public abstract IEnumerable<ISimpleAssociation> ExtractRelationships(TModel store);

        /// <summary>
        /// Refactor relationships
        /// </summary>
        public abstract void RefactorRelationships(List<IdentifiedData> item, Guid fromEntityKey, Guid toEntityKey);

        /// <summary>
        /// Get all MDM candidate locals regardless of where they are attached
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetAllMdmCandidateLocals();

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
        /// Gets all local associations between <paramref name="masterKey"/> and its master
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetAssociatedLocals(Guid masterKey);

        /// <summary>
        /// Get all candidate associations between <paramref name="masterKey"/> 
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetCandidateLocals(Guid masterKey);

        /// <summary>
        /// Get ignore associations
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetIgnoredCandidateLocals(Guid masterKey);

        /// <summary>
        /// Get all associations for which this is a candidate to another master
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetEstablishedCandidateMasters(Guid localKey);

        /// <summary>
        /// Get ignore associations
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetIgnoredMasters(Guid localKey);

        /// <summary>
        /// Get the master entity
        /// </summary>
        public abstract IMdmMaster MdmGet(Guid masterKey);

        /// <summary>
        /// Get a MDM Master for the specified local key
        /// </summary>
        public abstract IMdmMaster GetMasterFor(Guid localKey);

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
        /// Merge two master records together 
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMergeMasters(Guid survivorKey, Guid victimKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions to ignore candidate matches
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxIgnoreCandidateMatch(Guid hostKey, Guid ignoreKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create transaction instructions to establish MDM master link
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMasterLink(Guid fromKey, Guid toKey, IEnumerable<IdentifiedData> context, bool verified);

        /// <summary>
        /// Create transaction instructions to unlink a master
        /// </summary>
        public abstract IEnumerable<IdentifiedData> MdmTxMasterUnlink(Guid fromKey, Guid toKey, IEnumerable<IdentifiedData> context);

        /// <summary>
        /// Create a new master for the local
        /// </summary>
        public abstract IdentifiedData EstablishMasterFor(TModel local);

        /// <summary>
        /// Get all MDM associations for this local
        /// </summary>
        public abstract IEnumerable<ITargetedAssociation> GetAllMdmAssociations(Guid localKey);
    }
}
