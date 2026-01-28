/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using SanteDB.Core.Configuration;
using SanteDB.Core.Matching;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// A specialized match configuration service which wraps the already configured one
    /// </summary>
    public class MdmMatchConfigurationService : IRecordMatchingConfigurationService
    {
        /// <summary>
        /// MDM identity record match metadata
        /// </summary>
        private class MdmIdentityRecordMatchMetadata : IRecordMatchingConfigurationMetadata
        {
            /// <summary>
            /// Get the created by
            /// </summary>
            public string CreatedBy => "SYSTEM";

            /// <summary>
            /// Get the creation time
            /// </summary>
            public DateTimeOffset CreationTime => DateTimeOffset.Now;

            /// <summary>
            /// True if the configuration is read only
            /// </summary>
            public bool IsReadonly => true;

            /// <summary>
            /// Updated time
            /// </summary>
            public DateTimeOffset? UpdatedTime => null;

            /// <summary>
            /// Updated by
            /// </summary>
            public string UpdatedBy => null;

            /// <summary>
            /// Gets the state
            /// </summary>
            public MatchConfigurationStatus Status => MatchConfigurationStatus.Active;

            /// <summary>
            /// Gets the tags
            /// </summary>
            public IDictionary<string, string> Tags => new Dictionary<String, String>()
            {
                { MdmConstants.AutoLinkSetting, "true" }
            };
        }

        /// <summary>
        /// MDM identity record matching
        /// </summary>
        internal class MdmIdentityRecordMatchConfiguration : IRecordMatchingConfiguration
        {

            /// <summary>
            /// Serialization ctor
            /// </summary>
            public MdmIdentityRecordMatchConfiguration()
            {
                this.AppliesTo = new Type[0];
                this.Metadata = new MdmIdentityRecordMatchMetadata();
            }

            /// <summary>
            /// Create a new identity match configuration
            /// </summary>
            public MdmIdentityRecordMatchConfiguration(Type[] appliesTo)
            {
                this.AppliesTo = appliesTo;
                this.Metadata = new MdmIdentityRecordMatchMetadata();
            }

            /// <summary>
            /// Get the identity match uuid
            /// </summary>
            public Guid Uuid => MdmConstants.IdentityMatchUuid;

            /// <summary>
            /// Gets the name of this matching configuration
            /// </summary>
            public string Id => MdmConstants.MdmIdentityMatchConfiguration;

            /// <summary>
            /// Applies to types
            /// </summary>
            public Type[] AppliesTo { get; }

            /// <summary>
            /// Record matching metadata
            /// </summary>
            public IRecordMatchingConfigurationMetadata Metadata { get; set; }
        }

        // Matching configuration service wrapped
        private IRecordMatchingConfigurationService m_matchingConfigurationService;

        // Match configuration
        private readonly MdmIdentityRecordMatchConfiguration[] r_matchConfiguration;

        /// <summary>
        /// The match configuration service
        /// </summary>
        public MdmMatchConfigurationService(IConfigurationManager configurationManager, IRecordMatchingConfigurationService recordMatchingConfigurationService = null)
        {
            this.m_matchingConfigurationService = recordMatchingConfigurationService;
            this.r_matchConfiguration = new MdmIdentityRecordMatchConfiguration[] {
                new MdmIdentityRecordMatchConfiguration(configurationManager.GetSection<ResourceManagementConfigurationSection>().ResourceTypes.Select(o => o.Type).ToArray())
            };
        }

        /// <summary>
        /// Gets the configuration
        /// </summary>
        public IEnumerable<IRecordMatchingConfiguration> Configurations => this.m_matchingConfigurationService?.Configurations.Union(this.r_matchConfiguration) ?? this.r_matchConfiguration;

        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => "MDM Resource Merge Configuration";

        /// <summary>
        /// Delete a configuration
        /// </summary>
        public IRecordMatchingConfiguration DeleteConfiguration(String id) => this.m_matchingConfigurationService.DeleteConfiguration(id);

        /// <summary>
        /// Get configuration
        /// </summary>
        public IRecordMatchingConfiguration GetConfiguration(String id) => this.m_matchingConfigurationService.GetConfiguration(id);

        /// <summary>
        /// Save configuration
        /// </summary>
        public IRecordMatchingConfiguration SaveConfiguration(IRecordMatchingConfiguration configuration) => this.m_matchingConfigurationService.SaveConfiguration(configuration);

        /// <summary>
        /// Create identity match configuration
        /// </summary>
        internal static MdmIdentityRecordMatchConfiguration CreateIdentityMatchConfiguration<T>() => new MdmIdentityRecordMatchConfiguration(new Type[] { typeof(T) });
    }
}