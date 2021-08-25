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
using SanteDB.Core.Configuration;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Entities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// Class for constructing data managers
    /// </summary>
    public static class MdmDataManagerFactory
    {

        // Created instances
        private static ConcurrentDictionary<Type, Object> m_createdInstances = new ConcurrentDictionary<Type, object>();

        /// <summary>
        /// Create the specified data manager
        /// </summary>
        public static MdmDataManager<TModel> GetDataManager<TModel>()
            where TModel : IdentifiedData
        {
            if (m_createdInstances.TryGetValue(typeof(TModel), out object instance))
                return (MdmDataManager<TModel>)instance;
            return null;
        }

        /// <summary>
        /// Create the specified data manager
        /// </summary>
        public static MdmDataManager<TModel> GetDataManager<TModel>(Type forType)
            where TModel : IdentifiedData
        {
            if (m_createdInstances.TryGetValue(forType, out object instance))
                return (MdmDataManager<TModel>)instance;
            return null;
        }

        /// <summary>
        /// Register data manager
        /// </summary>
        internal static void RegisterDataManager(ResourceMergeConfiguration configuration)
        {
            if (configuration.MatchConfiguration == null || !configuration.MatchConfiguration.Any())
            {
                configuration.MatchConfiguration = new List<ResourceMergeMatchConfiguration>()
                {
                    new ResourceMergeMatchConfiguration(MdmConstants.MdmIdentityMatchConfiguration, true)
                };
            }

            if (typeof(Entity).IsAssignableFrom(configuration.ResourceType))
            {
                m_createdInstances.TryAdd(configuration.ResourceType, Activator.CreateInstance(typeof(MdmEntityDataManager<>).MakeGenericType(configuration.ResourceType), configuration));
            }
            else
            {
                throw new InvalidOperationException("Cannot create MDM data listener for this type");
            }
        }

        /// <summary>
        /// Create the data merger instance
        /// </summary>
        internal static Type CreateMerger(Type resourceType)
        {
            if (typeof(Entity).IsAssignableFrom(resourceType))
            {
                return typeof(MdmEntityMerger<>).MakeGenericType(resourceType);
            }
            throw new InvalidOperationException("Cannot create MDM data merger for this type");

        }

    }
}
