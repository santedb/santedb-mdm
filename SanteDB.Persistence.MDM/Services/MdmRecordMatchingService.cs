using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Represents a matching service that wraps the underlying system
    /// IRecordMatchingService and provides additional functionality
    /// </summary>
    public class MdmRecordMatchingService : IRecordMatchingService
    {

        // Match service
        private IRecordMatchingService m_matchService;

        // Unique authorities
        private IEnumerable<Guid> m_uniqueAuthorities;

        /// <summary>
        /// Existing match service
        /// </summary>
        public MdmRecordMatchingService(IDataPersistenceService<AssigningAuthority> authorityService, IRecordMatchingService existingMatchService = null)
        {
            this.m_matchService = existingMatchService;
            this.m_uniqueAuthorities = authorityService.Query(o => o.IsUnique, AuthenticationContext.SystemPrincipal).Select(o => o.Key.Value);
        }

        /// <summary>
        /// Service name
        /// </summary>
        public string ServiceName => "MDM Record Matching Service";

        /// <summary>
        /// Performs an identifier based match
        /// </summary>
        private IEnumerable<IRecordMatchResult<T>> PerformIdentityMatch<T>(T entity) where T : IdentifiedData
        {
            if (!(entity is IHasIdentifiers identifiers))
                throw new InvalidOperationException($"Cannot perform identity match on {typeof(T)}");


            // Identifiers in which entity has the unique authority
            var uqIdentifiers = identifiers.Identifiers.OfType<IExternalIdentifier>().Where(o => this.m_uniqueAuthorities.Contains(o.Authority?.Key ?? Guid.Empty));
            if (uqIdentifiers?.Any(i => i.Authority == null) == true)
                throw new InvalidOperationException("Some identifiers are missing authorities, cannot perform identity match");

            if (uqIdentifiers?.Any() != true)
                return new List<IRecordMatchResult<T>>();
            else
            {
                // TODO: Build this using Expression trees rather than relying on the parsing methods
                NameValueCollection nvc = new NameValueCollection();
                foreach (var itm in uqIdentifiers)
                    nvc.Add($"identifier[{itm.Authority.Key}].value", itm.Value);
                var filterExpression = QueryExpressionParser.BuildLinqExpression<T>(nvc);
                // Now we want to filter returning the masters
                using (AuthenticationContext.EnterSystemContext())
                {
                    var repository = ApplicationServiceContext.Current.GetService<IRepositoryService<T>>();
                    return repository.Find(filterExpression).Select(o => new MdmIdentityMatchResult<T>(o));
                }
            }
        }

        /// <summary>
        /// Perform a blocking stage setting
        /// </summary>
        public IEnumerable<T> Block<T>(T input, string configurationName) where T : IdentifiedData
        {
            return this.m_matchService?.Block<T>(input, configurationName);
        }

        /// <summary>
        /// Classify the records
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Classify<T>(T input, IEnumerable<T> blocks, string configurationName) where T : IdentifiedData
        {
            if (MdmConstants.MdmIdentityMatchConfiguration.Equals(configurationName))
                return this.PerformIdentityMatch(input);
            else
                return this.m_matchService?.Classify<T>(input, blocks, configurationName);
        }

        /// <summary>
        /// Perform a match
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Match<T>(T input, string configurationName) where T : IdentifiedData
        {
            if(MdmConstants.MdmIdentityMatchConfiguration.Equals(configurationName))
                return this.PerformIdentityMatch(input);
            else
                return this.m_matchService?.Match(input, configurationName);
        }

        /// <summary>
        /// Match 
        /// </summary>
        public IEnumerable<IRecordMatchResult> Match(IdentifiedData input, string configurationName)
        {
            // TODO: Provide a lookup list with a lambda expression to make this go faster
            var genMethod = typeof(MdmRecordMatchingService).GetGenericMethod(nameof(Match), new Type[] { input.GetType() }, new Type[] { input.GetType(), typeof(String) });
            var results = genMethod.Invoke(this, new object[] { input, configurationName }) as IEnumerable;
            return results.OfType<IRecordMatchResult>();
        }
    }
}
