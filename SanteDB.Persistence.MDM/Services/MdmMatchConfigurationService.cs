using SanteDB.Core.Configuration;
using SanteDB.Core.Matching;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            public MatchConfigurationStatus State => MatchConfigurationStatus.Active;

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
        private class MdmIdentityRecordMatchConfiguration : IRecordMatchingConfiguration
        {
            /// <summary>
            /// Create a new identity match configuration
            /// </summary>
            public MdmIdentityRecordMatchConfiguration(Type[] appliesTo)
            {
                this.AppliesTo = appliesTo;
                this.Metadata = new MdmIdentityRecordMatchMetadata();
            }

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
        public MdmMatchConfigurationService(IConfigurationManager configurationManager, IRecordMatchingConfigurationService recordMatchingConfigurationService)
        {
            this.m_matchingConfigurationService = recordMatchingConfigurationService;
            this.r_matchConfiguration = new MdmIdentityRecordMatchConfiguration[] {
            new MdmIdentityRecordMatchConfiguration(configurationManager.GetSection<ResourceManagementConfigurationSection>().ResourceTypes.Select(o => o.Type).ToArray())
            };
        }

        /// <summary>
        /// Gets the configuration
        /// </summary>
        public IEnumerable<IRecordMatchingConfiguration> Configurations => this.m_matchingConfigurationService.Configurations.Union(this.r_matchConfiguration);

        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => "MDM Resource Merge Configuration";

        /// <summary>
        /// Delete a configuration
        /// </summary>
        public IRecordMatchingConfiguration DeleteConfiguration(string name) => this.m_matchingConfigurationService.DeleteConfiguration(name);

        /// <summary>
        /// Get configuration
        /// </summary>
        public IRecordMatchingConfiguration GetConfiguration(string name) => this.m_matchingConfigurationService.GetConfiguration(name);

        /// <summary>
        /// Save configuration
        /// </summary>
        public IRecordMatchingConfiguration SaveConfiguration(IRecordMatchingConfiguration configuration) => this.m_matchingConfigurationService.SaveConfiguration(configuration);
    }
}