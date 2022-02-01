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
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Core.Matching;
using SanteDB.Persistence.MDM.Exceptions;
using SanteDB.Persistence.MDM.Services.Resources;
using SanteDB.Rest.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using RestSrvr;

namespace SanteDB.Persistence.MDM.Rest
{
    /// <summary>
    /// Exposees the $mdm-candidate API onto the REST layer
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class MdmCandidateOperation : IApiChildResourceHandler
    {
        private Tracer m_tracer = Tracer.GetTracer(typeof(MdmCandidateOperation));

        // Configuration
        private ResourceManagementConfigurationSection m_configuration;

        /// <summary>
        /// Candidate operations manager
        /// </summary>
        public MdmCandidateOperation(IConfigurationManager configurationManager)
        {
            this.m_configuration = configurationManager.GetSection<ResourceManagementConfigurationSection>();
            this.ParentTypes = this.m_configuration?.ResourceTypes.Select(o => o.Type).ToArray() ?? Type.EmptyTypes;
        }

        /// <summary>
        /// Gets the parent types
        /// </summary>
        public Type[] ParentTypes { get; }

        /// <summary>
        /// Gets the name of the resource
        /// </summary>
        public string Name => "mdm-candidate";

        /// <summary>
        /// Gets the type of properties which are returned
        /// </summary>
        public Type PropertyType => typeof(Bundle);

        /// <summary>
        /// Gets the capabilities of this
        /// </summary>
        public ResourceCapabilityType Capabilities => ResourceCapabilityType.Search | ResourceCapabilityType.Get | ResourceCapabilityType.Delete;

        /// <summary>
        /// Binding for this operation
        /// </summary>
        public ChildObjectScopeBinding ScopeBinding => ChildObjectScopeBinding.Instance | ChildObjectScopeBinding.Class;

        /// <summary>
        /// Re-Runs the matching algorithm on the specified master
        /// </summary>
        public object Add(Type scopingType, object scopingKey, object item)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the specified sub object
        /// </summary>
        public object Get(Type scopingType, object scopingKey, object key)
        {
            var matcher = ApplicationServiceContext.Current.GetService<IRecordMatchingService>();
            if (matcher == null)
            {
                throw new InvalidOperationException("No matching service configuration");
            }

            // Match report factory
            var matchReportFactory = ApplicationServiceContext.Current.GetService<IMatchReportFactory>();
            if (matchReportFactory == null)
            {
                throw new InvalidOperationException("No match report factory");
            }

            // Configuration provider
            var matchConfiguration = ApplicationServiceContext.Current.GetService<IRecordMatchingConfigurationService>();
            if (matchConfiguration == null)
            {
                throw new InvalidOperationException("No match configuration factory");
            }

            // Validate parameters
            if (scopingKey is Guid objectAKey && key is Guid objectBKey)
            {
                var repository = ApplicationServiceContext.Current.GetService(typeof(IRepositoryService<>).MakeGenericType(scopingType)) as IRepositoryService;

                // Produce a match report
                using (AuthenticationContext.EnterSystemContext())
                {
                    IdentifiedData recordA = repository.Get(objectAKey),
                        recordB = repository.Get(objectBKey);

                    if (recordA == null || recordB == null)
                    {
                        throw new KeyNotFoundException($"Source or target not found");
                    }

                    var configId = RestOperationContext.Current.IncomingRequest.QueryString["_configuration"];
                    IEnumerable<IRecordMatchingConfiguration> matchConfigurations = matchConfiguration.Configurations.Where(o => o.AppliesTo.Contains(scopingType) && o.Metadata.State == MatchConfigurationStatus.Active);
                    if (!String.IsNullOrEmpty(configId))
                    {
                        matchConfigurations = matchConfiguration.Configurations.Where(o => o.AppliesTo.Contains(scopingType) && o.Id == configId).Union(matchConfigurations);
                    }

                    if (matchConfiguration == null)
                    {
                        throw new InvalidOperationException("No configuration for type exists");
                    }

                    // Match result
                    var matchResult = matchConfigurations.SelectMany(c => matcher.Classify(recordA, new IdentifiedData[] { recordB }, c.Id));
                    return matchReportFactory.CreateMatchReport(scopingType, recordA, matchResult);
                }
            }
            else
            {
                throw new ArgumentException($"This request must be scoped to a single {scopingType.Name}");
            }
        }

        /// <summary>
        /// Query the candidate links
        /// </summary>
        public IEnumerable<object> Query(Type scopingType, object scopingKey, NameValueCollection filter, int offset, int count, out int totalCount)
        {
            var merger = ApplicationServiceContext.Current.GetService(typeof(IRecordMergingService<>).MakeGenericType(scopingType)) as IRecordMergingService;
            if (merger == null)
            {
                throw new InvalidOperationException("No merging service configuration");
            }

            IEnumerable<IdentifiedData> result = null;
            if (scopingKey == null) // TODO: This is being refactored to the new yield pattern this is just a temporary performance thing
            {
                result = merger.GetGlobalMergeCandidates(offset, count, out totalCount).OfType<IdentifiedData>();
            }
            else
            {
                result = merger.GetMergeCandidates((Guid)scopingKey);
                totalCount = result.Count();
                result = result.Skip(offset).Take(count);
            }

            return result;
        }

        /// <summary>
        /// Remove the specified key
        /// </summary>
        public object Remove(Type scopingType, object scopingKey, object key)
        {
            var merger = ApplicationServiceContext.Current.GetService(typeof(IRecordMergingService<>).MakeGenericType(scopingType)) as IRecordMergingService;
            if (merger == null)
            {
                throw new InvalidOperationException("No merging service configuration");
            }

            if (scopingKey is Guid scopingId && key is Guid keyId)
            {
                return merger.Ignore(scopingId, new Guid[] { keyId });
            }
            else
            {
                throw new ArgumentException($"Request must be scoped to a single {scopingType.Name}");
            }
        }
    }
}