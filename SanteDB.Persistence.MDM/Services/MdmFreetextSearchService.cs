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
 * Date: 2022-5-30
 */
using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using SanteDB.Persistence.MDM.Services.Resources;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Freetext search service that is Master Aware
    /// </summary>
    /// <remarks>Only use this freetext search service if your freetext search service implementation interacts directly with the
    /// SanteDB database, not if you're using something like Lucene or Redshift as those are index based and the fetch should
    /// be done via the IRepositoryService</remarks>
    public class MdmFreetextSearchService : IFreetextSearchService
    {
        /// <summary>
        /// Service name for the freetext search service
        /// </summary>
        public string ServiceName => "Basic MDM Freetext Search Service";

        // Configuration
        private ResourceManagementConfigurationSection m_configuration = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<ResourceManagementConfigurationSection>();

        /// <summary>
        /// Search for the specified entity
        /// </summary>
        public IQueryResultSet<TEntity> SearchEntity<TEntity>(string[] term) where TEntity : Entity, new()
        {

            // Perform the queries on the terms
            if (this.m_configuration.ResourceTypes.Any(rt => rt.Type == typeof(TEntity))) // Under MDM control
            {
                var principal = AuthenticationContext.Current.Principal;
                // HACK: Change this method to detect the type
                var idps = ApplicationServiceContext.Current.GetService<IDataPersistenceService<Entity>>();
                if (idps == null)
                    throw new InvalidOperationException("Cannot find a query repository service");

                var expression = QueryExpressionParser.BuildLinqExpression<Entity>(new NameValueCollection() {
                    { "classConcept", MdmConstants.MasterRecordClassification.ToString() },
                    { "relationship[97730a52-7e30-4dcd-94cd-fd532d111578].source.id", $":(freetext|{String.Join(" ", term)})" }
                });
                var results = idps.Query(expression, principal);
                return new MdmEntityResultSet<TEntity>(results, principal);
            }
            else
            {
                // Does the provider support freetext search clauses?
                var idps = ApplicationServiceContext.Current.GetService<IDataPersistenceService<TEntity>>();
                if (idps == null)
                    throw new InvalidOperationException("Cannot find a query repository service");

                var searchTerm = String.Join(" ", term);
                return idps.Query(o => o.FreetextSearch(searchTerm), AuthenticationContext.Current.Principal);
            }
        }
    }
}