using SanteDB.Core.Configuration;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Interop;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Services;
using SanteDB.Rest.Common;
using System;
using System.Linq;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// MDM Operation Base
    /// </summary>
    public abstract class MdmOperationBase : IApiChildOperation
    {

        // Operation base
        protected Tracer m_tracer = Tracer.GetTracer(typeof(MdmOperationBase));

        // Configuration
        protected ResourceMergeConfigurationSection m_configuration;

        // Batch service
        protected IRepositoryService<Bundle> m_batchService;

        /// <summary>
        /// Get the binding
        /// </summary>
        public abstract ChildObjectScopeBinding ScopeBinding { get; }

        /// <summary>
        /// Gets the parents type
        /// </summary>
        public Type[] ParentTypes { get; }

        /// <summary>
        /// Gets the name of the operation
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Candidate operations manager
        /// </summary>
        public MdmOperationBase(IConfigurationManager configurationManager, IRepositoryService<Bundle> batchService)
        {
            this.m_configuration = configurationManager.GetSection<ResourceMergeConfigurationSection>();
            this.ParentTypes = this.m_configuration?.ResourceTypes.Select(o => o.ResourceType).ToArray() ?? Type.EmptyTypes;
            this.m_batchService = batchService;
        }

        /// <summary>
        /// Invoke the operation
        /// </summary>
        public abstract object Invoke(Type scopingType, object scopingKey, ApiOperationParameterCollection parameters);
    }
}