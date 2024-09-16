/*
 * Copyright (C) 2021 - 2024, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 */
using SanteDB.Core.Configuration;
using SanteDB.Docker.Core;
using SanteDB.Persistence.MDM.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace SanteDB.Persistence.MDM.Docker
{
    /// <summary>
    /// MDM Docker feature
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class MdmDockerFeature : IDockerFeature
    {
        /// <summary>
        /// Identifies the resource
        /// </summary>
        public const string ResourceTypeSetting = "RESOURCE";

        /// <summary>
        /// Get the id of this docker feature
        /// </summary>
        public string Id => "MDM";

        /// <summary>
        /// Gets the settings
        /// </summary>
        public IEnumerable<string> Settings => new String[] { ResourceTypeSetting };

        /// <summary>
        /// Configure the feature
        /// </summary>
        public void Configure(SanteDBConfiguration configuration, IDictionary<string, string> settings)
        {
            var resourceConf = configuration.GetSection<ResourceManagementConfigurationSection>();
            if (resourceConf == null)
            {
                resourceConf = new ResourceManagementConfigurationSection();
                configuration.AddSection(resourceConf);
            }

            if (settings.TryGetValue(ResourceTypeSetting, out string resources))
            {
                resourceConf.ResourceTypes = resources.Split(';').Select(o =>
                {
                    var conf = o.Split('=');

                    if (conf.Length > 1)
                    {
                        Trace.TraceWarning("OLD SETTING DETECTED - Since 2.1.65 the of match configurations ({0}) should be moved to the match definition - THIS SETTING WILL BE IGNORED", conf[1]);
                    }

                    return new ResourceTypeReferenceConfiguration()
                    {
                        TypeXml = conf[0]
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