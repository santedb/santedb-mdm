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
