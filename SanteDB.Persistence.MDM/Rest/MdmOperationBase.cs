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