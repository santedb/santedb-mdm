using SanteDB.Core.Configuration;
using SanteDB.Docker.Core;
using SanteDB.Persistence.MDM.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Docker
{
    /// <summary>
    /// MDM Docker feature
    /// </summary>
    public class MdmDockerFeature : IDockerFeature
    {

        /// <summary>
        /// Identifies the resource
        /// </summary>
        public const string ResourceTypeSetting = "RESOURCE";

        /// <summary>
        /// Auto merge setting
        /// </summary>
        public const string AutoMergeSetting = "AUTO_MERGE";

        /// <summary>
        /// Get the id of this docker feature
        /// </summary>
        public string Id => "MDM";

        /// <summary>
        /// Gets the settings
        /// </summary>
        public IEnumerable<string> Settings => new String[] { AutoMergeSetting, ResourceTypeSetting };

        /// <summary>
        /// Configure the feature
        /// </summary>
        public void Configure(SanteDBConfiguration configuration, IDictionary<string, string> settings)
        {
            var resourceConf = configuration.GetSection<ResourceMergeConfigurationSection>();
            if (resourceConf == null)
            {
                resourceConf = new ResourceMergeConfigurationSection();
                configuration.AddSection(resourceConf);
            }

            // Action settings
            bool autoMerge = false;
            if (settings.TryGetValue(AutoMergeSetting, out string auto))
            {
                if (!Boolean.TryParse(auto, out autoMerge))
                {
                    throw new ArgumentOutOfRangeException($"{auto} is not a valid boolean");
                }
            }

            if (settings.TryGetValue(ResourceTypeSetting, out string resources))
            {
                resourceConf.ResourceTypes = resources.Split(';').Select(o => {
                    var conf = o.Split('=');
                    return new ResourceMergeConfiguration()
                    {
                        ResourceTypeXml = conf[0],
                        AutoMerge = autoMerge,
                        MatchConfiguration = new List<string>() { conf[1] }
                    };
                }).ToList();
            }

            // Add services
            var serviceConfiguration = configuration.GetSection<ApplicationServiceContextConfigurationSection>().ServiceProviders;
            if (!serviceConfiguration.Any(s => s.Type == typeof(MdmDataManagementService)))
            {
                serviceConfiguration.Add(new TypeReferenceConfiguration(typeof(MdmDataManagementService)));
            }
        }
    }
}
