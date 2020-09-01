using SanteDB.Core;
using SanteDB.Core.Configuration;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
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
        private ResourceMergeConfigurationSection m_configuration = ApplicationServiceContext.Current.GetService<IConfigurationManager>().GetSection<ResourceMergeConfigurationSection>();

        /// <summary>
        /// Search for the specified entity
        /// </summary>
        public IEnumerable<TEntity> Search<TEntity>(string[] term, Guid queryId, int offset, int? count, out int totalResults, ModelSort<TEntity>[] orderBy) where TEntity : IdentifiedData, new()
        {
            // Perform the queries on the terms
            var mdmListener = ApplicationServiceContext.Current.GetService<MdmResourceListener<TEntity>>();
            if (this.m_configuration.ResourceTypes.Any(rt => rt.ResourceType == typeof(TEntity))) // Under MDM control
            {
                var idps = ApplicationServiceContext.Current.GetService<IUnionQueryDataPersistenceService<Entity>>();
                if (idps == null)
                    throw new InvalidOperationException("Cannot find a UNION query repository service");

                var searchFilters = new List<Expression<Func<Entity, bool>>>(term.Length);
                searchFilters.Add(QueryExpressionParser.BuildLinqExpression<Entity>(new NameValueCollection() { { "classConcept", MdmConstants.MasterRecordClassification.ToString() }, { "identifier.value", term } }));
                searchFilters.Add(QueryExpressionParser.BuildLinqExpression<Entity>(new NameValueCollection() { { "classConcept", MdmConstants.MasterRecordClassification.ToString() }, { $"relationship[MDM-Master].source.name.component.value", term.Select(o => $":(approx|\"{o}\")") } }));
                searchFilters.Add(QueryExpressionParser.BuildLinqExpression<Entity>(new NameValueCollection() { { "classConcept", MdmConstants.MasterRecordClassification.ToString() }, { $"relationship[MDM-Master].source.identifier.value", term } }));
                var results = idps.Union(searchFilters.ToArray(), queryId, offset, count, out totalResults, AuthenticationContext.Current.Principal);
                return results.AsParallel().AsOrdered().Select(o => o is Entity ? new EntityMaster<TEntity>((Entity)(object)o).GetMaster(AuthenticationContext.Current.Principal) : new ActMaster<TEntity>((Act)(Object)o).GetMaster(AuthenticationContext.Current.Principal)).OfType<TEntity>().ToList();
            }
            else
            {
                var idps = ApplicationServiceContext.Current.GetService<IUnionQueryDataPersistenceService<TEntity>>();
                if (idps == null)
                    throw new InvalidOperationException("Cannot find a UNION query repository service");
                var searchFilters = new List<Expression<Func<TEntity, bool>>>(term.Length);
                searchFilters.Add(QueryExpressionParser.BuildLinqExpression<TEntity>(new NameValueCollection() { { "name.component.value", term.Select(o => $":(approx|\"{o}\")") } } ));
                searchFilters.Add(QueryExpressionParser.BuildLinqExpression<TEntity>(new NameValueCollection() { { "identifier.value", term } }));
                return idps.Union(searchFilters.ToArray(), queryId, offset, count, out totalResults, AuthenticationContext.Current.Principal, orderBy);
            }

        }
    }
}
