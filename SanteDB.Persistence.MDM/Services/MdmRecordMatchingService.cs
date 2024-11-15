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
using SanteDB.Core;
using SanteDB.Core.Matching;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

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
        private ICollection<Guid> m_uniqueAuthorities;
        // Unique authorities domain
        private ICollection<string> m_uniqueAuthoritiesDomain;

        // Entity relationship
        private IDataPersistenceService<EntityRelationship> m_erService;

        // Act relationship
        private IDataPersistenceService<ActRelationship> m_arService;

        /// <summary>
        /// Existing match service
        /// </summary>
        public MdmRecordMatchingService(IDataPersistenceService<IdentityDomain> authorityService, IDataPersistenceService<Bundle> bundleService, IDataPersistenceService<EntityRelationship> erService, IDataPersistenceService<ActRelationship> arService, IRecordMatchingService existingMatchService = null)
        {
            this.m_matchService = existingMatchService;
            this.m_erService = erService;
            this.m_arService = arService;
            this.m_uniqueAuthorities = authorityService.Query(o => o.IsUnique, AuthenticationContext.SystemPrincipal).Select(o => o.Key.Value).ToList();
            this.m_uniqueAuthoritiesDomain = authorityService.Query(o => o.IsUnique, AuthenticationContext.SystemPrincipal).Select(o => o.DomainName).ToList();
            bundleService.Inserted += (o, e) =>
            {
                foreach (var i in e.Data.Item.OfType<IdentityDomain>())
                {
                    if (i.BatchOperation == BatchOperationType.Delete || i.ObsoletionTime.HasValue)
                    {
                        this.m_uniqueAuthorities.Remove(i.Key.Value);
                        this.m_uniqueAuthoritiesDomain.Remove(i.DomainName);
                    }
                    else if (i.IsUnique)
                    {
                        this.m_uniqueAuthorities.Add(i.Key.Value);
                        this.m_uniqueAuthoritiesDomain.Add(i.DomainName);
                    }
                }
            };
            authorityService.Inserted += (o, e) =>
            {
                if (e.Data.IsUnique)
                {
                    this.m_uniqueAuthorities.Add(e.Data.Key.Value);
                }
            };
            authorityService.Deleted += (o, e) =>
            {
                this.m_uniqueAuthorities.Remove(e.Data.Key.Value);
            };
        }

        /// <summary>
        /// Service name
        /// </summary>
        public string ServiceName => "MDM Record Matching Service";

        /// <summary>
        /// Get the ignore list
        /// </summary>
        private IEnumerable<Guid> GetIgnoreList<T>(IEnumerable<Guid> userProvidedList, T input)
            where T : IdentifiedData
        {
            userProvidedList = userProvidedList ?? new Guid[0];

            if (typeof(Entity).IsAssignableFrom(typeof(T)))
            {
                return userProvidedList.Union(this.m_erService.Query(o => o.SourceEntityKey == input.Key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Select(o => o.Key.Value));
            }
            else if (typeof(Act).IsAssignableFrom(typeof(T)))
            {
                return userProvidedList.Union(this.m_arService.Query(o => o.SourceEntityKey == input.Key && o.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship && o.ObsoleteVersionSequenceId == null, AuthenticationContext.SystemPrincipal).Select(o => o.Key.Value));
            }
            else
            {
                throw new InvalidOperationException();
            }
        }

        /// <summary>
        /// Performs an identifier based match
        /// </summary>
        private IEnumerable<IRecordMatchResult<T>> PerformIdentityMatch<T>(T entity, IEnumerable<Guid> ignoreKeys, IRecordMatchingDiagnosticSession collector) where T : IdentifiedData
        {
            if (!(entity is IHasIdentifiers identifiers))
            {
                throw new InvalidOperationException($"Cannot perform identity match on {typeof(T)}");
            }

            collector?.LogStartStage("blocking");
            // Identifiers in which entity has the unique authority
            var uqIdentifiers = identifiers.LoadProperty(o => o.Identifiers).OfType<IExternalIdentifier>().Where(o => this.m_uniqueAuthorities.Contains(o.IdentityDomain.Key ?? Guid.Empty) || this.m_uniqueAuthoritiesDomain.Contains(o.IdentityDomain.DomainName));
            if (uqIdentifiers?.Any(i => i.IdentityDomain == null) == true)
            {
                throw new InvalidOperationException("Some identifiers are missing authorities, cannot perform identity match");
            }

            if (uqIdentifiers?.Any() != true)
            {
                return new List<IRecordMatchResult<T>>();
            }
            else
            {
                try
                {
                    collector?.LogStartAction("block-identity");
                    // TODO: Build this using Expression trees rather than relying on the parsing methods
                    //var filterParameter = Expression.Parameter(typeof(T));
                    //Expression identityFilterExpresion = null;
                    //foreach(var itm in uqIdentifiers)
                    //{
                    //    Expression identityCheck = Expression.MakeMemberAccess(filterParameter, typeof(T).GetProperty(nameof(Entity.Identifiers)));
                    //    identityCheck = Expression.Call(typeof(Enumerable).GetGenericMethod(nameof(Enumerable.Where), new Type[] { identityCheck.Type.StripGeneric() }, 
                    //}
                    var nvc = new NameValueCollection();
                    foreach (var itm in uqIdentifiers)
                    {
                        nvc.Add($"identifier[{itm.IdentityDomain.Key?.ToString() ?? itm.IdentityDomain.DomainName}].value", itm.Value);
                    }

                    var filterExpression = QueryExpressionParser.BuildLinqExpression<T>(nvc);
                    // Now we want to filter returning the masters
                    using (AuthenticationContext.EnterSystemContext())
                    {
                        var repository = ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>();
                        var retVal = repository.Query(filterExpression, AuthenticationContext.SystemPrincipal).Where(o => !ignoreKeys.Contains(o.Key.Value)).OfType<T>().Select(o => new MdmIdentityMatchResult<T>(entity, o, RecordMatchClassification.Match));
                        collector?.LogSample(filterExpression.ToString(), retVal.Count());
                        return retVal;
                    }
                }
                finally
                {
                    collector?.LogEnd();
                }
            }
        }


        /// <summary>
        /// Perform a blocking stage setting
        /// </summary>
        public IQueryResultSet<T> Block<T>(T input, string configurationName, IEnumerable<Guid> ignoreKeys, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            if (MdmConstants.MdmIdentityMatchConfiguration.Equals(configurationName))
            {
                return this.PerformIdentityMatch(input, this.GetIgnoreList(ignoreKeys, input), collector).Select(o => o.Record).AsResultSet<T>();
            }
            else
            {
                return this.m_matchService?.Block<T>(input, configurationName, this.GetIgnoreList(ignoreKeys, input));
            }
        }

        /// <summary>
        /// Classify the records
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Classify<T>(T input, IEnumerable<T> blocks, string configurationName, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            if (MdmConstants.MdmIdentityMatchConfiguration.Equals(configurationName))
            {
                return this.PerformIdentityClassify(input, blocks, collector);
            }
            else
            {
                return this.m_matchService?.Classify<T>(input, blocks, configurationName);
            }
        }

        /// <summary>
        /// Perform a classification operation on identifier
        /// </summary>
        private IEnumerable<IRecordMatchResult<T>> PerformIdentityClassify<T>(T input, IEnumerable<T> blocks, IRecordMatchingDiagnosticSession collector) where T : IdentifiedData
        {
            try
            {
                collector?.LogStartStage("scoring");

                if (!(input is IHasIdentifiers identifiers))
                {
                    throw new InvalidOperationException($"Cannot perform identity match on {typeof(T)}");
                }

                // Identifiers in which entity has the unique authority
                var uqIdentifiers = identifiers.Identifiers.OfType<IExternalIdentifier>().Where(o => this.m_uniqueAuthorities.Contains(o.IdentityDomain.Key ?? Guid.Empty));
                if (uqIdentifiers?.Any(i => i.IdentityDomain.Key == null) == true)
                {
                    throw new InvalidOperationException("Some identifiers are missing authorities, cannot perform identity match");
                }

                if (uqIdentifiers?.Any() != true)
                {
                    return blocks.Select(o => new MdmIdentityMatchResult<T>(input, o, RecordMatchClassification.NonMatch, 0.0f));
                }
                else
                {
                    return blocks.Select(o =>
                    {
                        try
                        {
                            collector?.LogStartAction(o);

                            if (o is IHasIdentifiers oid)
                            {
                                var isMatch = oid.Identifiers.Any(i => uqIdentifiers.Any(u => u.IdentityDomain.Key == i.IdentityDomain.Key && i.Value == u.Value));

                                return new MdmIdentityMatchResult<T>(input, o, isMatch ? RecordMatchClassification.Match : RecordMatchClassification.NonMatch, isMatch ? 1.0f : 0.0f);
                            }
                            else
                            {
                                return new MdmIdentityMatchResult<T>(input, o, RecordMatchClassification.NonMatch, 0.0f);
                            }
                        }
                        finally
                        {
                            collector?.LogEndAction();
                        }
                    });
                }
            }
            finally
            {
                collector?.LogEndStage();
            }
        }

        /// <summary>
        /// Perform a match
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Match<T>(T input, string configurationName, IEnumerable<Guid> ignoreKeys, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            // Fetch ignore keys if none provided

            if (MdmConstants.MdmIdentityMatchConfiguration.Equals(configurationName))
            {
                return this.PerformIdentityMatch(input, this.GetIgnoreList(ignoreKeys, input), collector);
            }
            else
            {
                return this.m_matchService?.Match(input, configurationName, this.GetIgnoreList(ignoreKeys, input), collector);
            }
        }

        /// <summary>
        /// Match
        /// </summary>
        public IEnumerable<IRecordMatchResult> Match(IdentifiedData input, string configurationName, IEnumerable<Guid> ignoreKeys, IRecordMatchingDiagnosticSession collector = null)
        {
            // TODO: Provide a lookup list with a lambda expression to make this go faster
            var genMethod = typeof(MdmRecordMatchingService).GetGenericMethod(nameof(Match), new Type[] { input.GetType() }, new Type[] { input.GetType(), typeof(String), typeof(IEnumerable<Guid>), typeof(IRecordMatchingDiagnosticSession) });
            var results = genMethod.Invoke(this, new object[] { input, configurationName, ignoreKeys, collector }) as IEnumerable;
            return results.OfType<IRecordMatchResult>();
        }

        /// <summary>
        /// Classify
        /// </summary>
        public IEnumerable<IRecordMatchResult> Classify(IdentifiedData input, IEnumerable<IdentifiedData> blocks, String configurationName, IRecordMatchingDiagnosticSession collector = null)
        {
            var genMethod = typeof(MdmRecordMatchingService).GetGenericMethod(nameof(Classify), new Type[] { input.GetType() }, new Type[] { input.GetType(), typeof(IEnumerable<>).MakeGenericType(input.GetType()), typeof(String), typeof(IRecordMatchingDiagnosticSession) });
            var ofTypeMethod = typeof(Enumerable).GetGenericMethod(nameof(Enumerable.OfType), new Type[] { input.GetType() }, new Type[] { typeof(IEnumerable) });
            var results = genMethod.Invoke(this, new object[] { input, ofTypeMethod.Invoke(null, new object[] { blocks }), configurationName, collector }) as IEnumerable;
            return results.OfType<IRecordMatchResult>();
        }

        /// <summary>
        /// Create a diagnostics session collector
        /// </summary>
        public IRecordMatchingDiagnosticSession CreateDiagnosticSession()
        {
            return this.m_matchService.CreateDiagnosticSession();
        }
    }
}