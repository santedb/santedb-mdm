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

using SanteDB.Core.Interop;
using SanteDB.Core.Jobs;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Jobs;
using SanteDB.Persistence.MDM.Services.Resources;
using SanteDB.Rest.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// Represents a re-linking operation
    /// </summary>
    /// <remarks>This operation forces a re-matching operation</remarks>
    public class MdmMatchOperation : MdmOperationBase
    {
        // Job manager
        private IJobManagerService m_jobManager;

        /// <summary>
        /// Gets the binding
        /// </summary>
        public override ChildObjectScopeBinding ScopeBinding => ChildObjectScopeBinding.Class | ChildObjectScopeBinding.Instance;

        /// <summary>
        /// Creates a new configuration match operation
        /// </summary>
        public MdmMatchOperation(IConfigurationManager configurationManager, IJobManagerService jobManager, IDataPersistenceService<Bundle> batchService) : base(configurationManager, batchService)
        {
            this.m_jobManager = jobManager;
        }

        /// <summary>
        /// Gets the name of the operation
        /// </summary>
        public override string Name => "mdm-rematch";

        /// <summary>
        /// Perform  the operation
        /// </summary>
        public override object Invoke(Type scopingType, object scopingKey, ApiOperationParameterCollection parameters)
        {
            var dataManager = MdmDataManagerFactory.GetDataManager(scopingType);
            if (dataManager == null)
            {
                throw new NotSupportedException($"MDM is not configured for {scopingType}");
            }

            if (scopingKey == null)
            {
                parameters.TryGet<bool>("clear", out bool clear);
                this.m_jobManager.StartJob(typeof(MdmMatchJob<>).MakeGenericType(scopingType), new object[] { clear });
                return null;
            }
            else if (scopingKey is Guid scopingObjectKey)
            {
                // Load the current master from @scopingKey
                if (!dataManager.IsMaster(scopingObjectKey))
                {
                    throw new KeyNotFoundException($"{scopingObjectKey} is not an MDM Master");
                }

                // Now - we want to prepare a transaction
                Bundle retVal = new Bundle();

                if (parameters.TryGet<bool>("clear", out bool clear) && clear)
                {
                    foreach (var itm in dataManager.GetCandidateLocals(scopingObjectKey).Where(o => o.ClassificationKey == MdmConstants.AutomagicClassification))
                    {
                        if (itm is EntityRelationship er)
                        {
                            er.BatchOperation = Core.Model.DataTypes.BatchOperationType.Delete;
                            retVal.Add(er);
                        }
                        else if (itm is ActRelationship ar)
                        {
                            ar.BatchOperation = Core.Model.DataTypes.BatchOperationType.Delete;
                            retVal.Add(ar);
                        }
                    }
                }

                retVal.AddRange(dataManager.MdmTxDetectCandidates(dataManager.MdmGet(scopingObjectKey).GetMaster(AuthenticationContext.Current.Principal) as Entity, retVal.Item));

                // Now we want to save?
                try
                {
                    retVal = this.m_batchService.Insert(retVal, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error persisting re-match: {0}", e.Message);
                    throw new MdmException("Error persisting re-match operation", e);
                }
                return retVal;
            }
            else
            {
                throw new InvalidOperationException("Cannot determine the operation");
            }
        }
    }
}