/*
 * Copyright (C) 2021 - 2022, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-10-29
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Interop;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Security.Audit;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Services.Resources;
using SanteDB.Rest.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// Exposees the $mdm-candidate API onto the REST layer
    /// </summary>
    [ExcludeFromCodeCoverage] // REST operations require a REST client to test
    public class MdmLinkResource : IApiChildResourceHandler
    {
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(MdmLinkResource));

        // Configuration
        private ResourceManagementConfigurationSection m_configuration;

        // Batch service
        private IDataPersistenceService<Bundle> m_batchService;

        /// <summary>
        /// Candidate operations manager
        /// </summary>
        public MdmLinkResource(IConfigurationManager configurationManager, IDataPersistenceService<Bundle> batchService)
        {
            this.m_configuration = configurationManager.GetSection<ResourceManagementConfigurationSection>();
            this.ParentTypes = this.m_configuration?.ResourceTypes.Select(o => o.Type).ToArray() ?? Type.EmptyTypes;
            this.m_batchService = batchService;
        }

        /// <summary>
        /// Gets the parent types
        /// </summary>
        public Type[] ParentTypes { get; }

        /// <summary>
        /// Gets the name of the resource
        /// </summary>
        public string Name => "mdm-link";

        /// <summary>
        /// Gets the type of properties which are returned
        /// </summary>
        public Type PropertyType => typeof(Bundle);

        /// <summary>
        /// Gets the capabilities of this
        /// </summary>
        public ResourceCapabilityType Capabilities => ResourceCapabilityType.Create | ResourceCapabilityType.Search | ResourceCapabilityType.Delete;

        /// <summary>
        /// Binding for this operation
        /// </summary>
        public ChildObjectScopeBinding ScopeBinding => ChildObjectScopeBinding.Instance | ChildObjectScopeBinding.Class;

        /// <summary>
        /// Attach a master
        /// </summary>
        public object Add(Type scopingType, object scopingKey, object item)
        {
            var dataManager = MdmDataManagerFactory.GetDataManager(scopingType);
            if (dataManager == null)
            {
                throw new NotSupportedException($"MDM is not configured for {scopingType}");
            }

            // Detach
            if (scopingKey is Guid scopedKey && item is IdentifiedDataReference childObject)
            {
                try
                {
                    var transaction = new Bundle(dataManager.MdmTxMasterLink(scopedKey, childObject.Key.Value, new IdentifiedData[0], true));

                    var retVal = this.m_batchService.Insert(transaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                    return retVal;
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error attaching master- {0}", e);
                    throw new MdmException($"Error detaching {scopingKey} from {childObject.Key}", e);
                }
            }
            else
            {
                throw new ArgumentException("Invalid URL path or Reference object");
            }
        }

        /// <summary>
        /// Gets the specified sub object
        /// </summary>
        public object Get(Type scopingType, object scopingKey, object key)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Query the candidate links
        /// </summary>
        public IQueryResultSet Query(Type scopingType, object scopingKey, NameValueCollection filter)
        {
            var dataManager = MdmDataManagerFactory.GetDataManager(scopingType);
            if (dataManager == null)
            {
                throw new NotSupportedException($"MDM is not configured for {scopingType}");
            }

            if (scopingKey is Guid scopedKey)
            {
                try
                {
                    if (dataManager.IsMaster(scopedKey)) // Get all locals
                    {
                        // Translate the filter
                        // TODO: Filtering and sorting for the associated locals call
                        var associatedLocals = dataManager.GetAssociatedLocals(scopedKey);
                        return new NestedQueryResultSet(associatedLocals, o =>
                        {
                            if (o is ITargetedAssociation ta)
                            {
                                var tag = ta.LoadProperty(p => p.SourceEntity) as ITaggable;
                                if (ta.ClassificationKey == MdmConstants.AutomagicClassification)
                                {
                                    tag.AddTag(MdmConstants.MdmClassificationTag, "Auto");
                                }
                                else if (ta.ClassificationKey == MdmConstants.SystemClassification)
                                {
                                    tag.AddTag(MdmConstants.MdmClassificationTag, "System");
                                }
                                else
                                {
                                    tag.AddTag(MdmConstants.MdmClassificationTag, "Verified");
                                }

                                return ta.SourceEntity;
                            }
                            else
                            {
                                return null;
                            }
                        });
                    }
                    else
                    {
                        return new MemoryQueryResultSet(new object[] { dataManager.GetMasterRelationshipFor(scopedKey).LoadProperty(o => o.TargetEntity) });
                    }
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error querying for locals on {0} - {1}", scopedKey, e);
                    throw new MdmException($"Error querying for locals for {scopedKey}", e);
                }
            }
            else
            {
                throw new InvalidOperationException("UUID must be passed as scoper key");
            }
        }

        /// <summary>
        /// Detach from master
        /// </summary>
        public object Remove(Type scopingType, object scopingKey, object key)
        {
            var dataManager = MdmDataManagerFactory.GetDataManager(scopingType);
            if (dataManager == null)
            {
                throw new NotSupportedException($"MDM is not configured for {scopingType}");
            }

            // Detach
            if (scopingKey is Guid scopedKey && key is Guid childKey)
            {
                try
                {
                    var transaction = new Bundle(dataManager.MdmTxMasterUnlink(scopedKey, childKey, new IdentifiedData[0]));

                    var retVal = this.m_batchService.Insert(transaction, TransactionMode.Commit, AuthenticationContext.Current.Principal);
                    return retVal;
                }
                catch (Exception e)
                {
                    this.m_tracer.TraceError("Error detaching master- {0}", e);

                    throw new MdmException($"Error detaching {scopingKey} from {childKey}", e);
                }
            }
            else
            {
                throw new ArgumentException("Arguments must be UUID");
            }
        }
    }
}