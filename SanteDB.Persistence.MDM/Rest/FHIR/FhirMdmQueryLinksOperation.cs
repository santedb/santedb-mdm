/*
 * Copyright (C) 2021 - 2023, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-3-10
 */
using Hl7.Fhir.Model;
using RestSrvr;
using SanteDB.Core;
using SanteDB.Core.Matching;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Services;
using SanteDB.Messaging.FHIR.Extensions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Expression = System.Linq.Expressions.Expression;
using Patient = SanteDB.Core.Model.Roles.Patient;

namespace SanteDB.Persistence.MDM.Rest.FHIR
{
    /// <summary>
    /// Represents a FHIR MDM query links operation.
    /// </summary>
    [ExcludeFromCodeCoverage] // REST operations require a REST client to test
    public class FhirMdmQueryLinksOperation : IFhirOperationHandler
    {
        /// <summary>
        /// Gets the name of the operation.
        /// </summary>
        public string Name => "mdm-query-links";

        /// <summary>
        /// Gets the URI where this operation is defined.
        /// </summary>
        public Uri Uri => new Uri("OperationDefinition/mdm-query-links", UriKind.Relative);

        /// <summary>
        /// The type that this operation handler applies to (or null if it applies to all)
        /// </summary>
        public ResourceType[] AppliesTo => new[]
        {
            ResourceType.Patient
        };

        /// <summary>
        /// Get the parameter list for this object.
        /// </summary>
        public IDictionary<string, FHIRAllTypes> Parameters => new Dictionary<string, FHIRAllTypes>
        {
            { "goldenResourceId", FHIRAllTypes.String },
            { "resourceId", FHIRAllTypes.String },
            { "_count", FHIRAllTypes.Integer },
            { "linkSource", FHIRAllTypes.String },
            { "matchResult", FHIRAllTypes.String },
            { "_offset", FHIRAllTypes.Integer },
            { "_configurationName", FHIRAllTypes.String }
        };

        /// <summary>
        /// The link source map.
        /// </summary>
        private static readonly Dictionary<string, Guid> linkSourceMap = new Dictionary<string, Guid>
        {
            { "AUTO", MdmConstants.AutomagicClassification },
            { "MANUAL", MdmConstants.VerifiedClassification }
        };

        /// <summary>
        /// The match result source map.
        /// </summary>
        private static readonly Dictionary<string, Guid> matchResultMap = new Dictionary<string, Guid>
        {
            { "MATCH",  MdmConstants.MasterRecordRelationship },
            { "POSSIBLE_MATCH", MdmConstants.CandidateLocalRelationship },
            { "NO_MATCH", MdmConstants.IgnoreCandidateRelationship }
        };

        /// <summary>
        /// True if the operation impacts the object state.
        /// </summary>
        public bool IsGet => true;

        /// <summary>
        /// Invoke the specified operation.
        /// </summary>
        /// <param name="parameters">The parameter set to action.</param>
        /// <returns>The result of the operation.</returns>
        public Resource Invoke(Parameters parameters)
        {
            var configuration = RestOperationContext.Current.IncomingRequest.QueryString["_configurationName"];

            // validate query parameters
            if (string.IsNullOrEmpty(configuration))
            {
                throw new InvalidOperationException("No resource merge configuration specified. Use the ?_configurationName parameter specify a configuration.");
            }

            var countParameter = RestOperationContext.Current.IncomingRequest.QueryString["_count"];
            var offsetParameter = RestOperationContext.Current.IncomingRequest.QueryString["_offset"];
            uint offset = 0;
            uint count = 1000;

            if (!string.IsNullOrEmpty(offsetParameter) && !uint.TryParse(offsetParameter, out offset))
            {
                throw new InvalidOperationException("Invalid value for _offset. The _offset value must be a positive integer.");
            }

            if (!string.IsNullOrEmpty(countParameter) && !uint.TryParse(countParameter, out count))
            {
                throw new InvalidOperationException("Invalid value for _count. The _count value must be a positive integer.");
            }

            var linkSource = RestOperationContext.Current.IncomingRequest.QueryString["linkSource"]?.ToUpperInvariant();
            var matchResult = RestOperationContext.Current.IncomingRequest.QueryString["matchResult"]?.ToUpperInvariant();
            var masterResourceId = RestOperationContext.Current.IncomingRequest.QueryString["goldenResourceId"]?.ToUpperInvariant();
            var resourceId = RestOperationContext.Current.IncomingRequest.QueryString["resourceId"]?.ToUpperInvariant();

            if (!string.IsNullOrEmpty(linkSource) && !linkSourceMap.ContainsKey(linkSource))
            {
                throw new InvalidOperationException($"Invalid value for linkSource: '{linkSource}'. The link source must be one of the following values: {string.Join(", ", linkSourceMap.Keys)}.");
            }

            if (!string.IsNullOrEmpty(matchResult) && !matchResultMap.ContainsKey(matchResult))
            {
                throw new InvalidOperationException($"Invalid value for matchResult: '{matchResult}'. The match result must be one of the following values: {string.Join(", ", matchResultMap.Keys)}.");
            }

            var matchingService = ApplicationServiceContext.Current.GetService<IRecordMatchingService>();

            if (matchingService == null)
            {
                throw new InvalidOperationException("No record matching service found");
            }

            var entityRelationshipService = ApplicationServiceContext.Current.GetService<IRepositoryService<EntityRelationship>>();
            var patientService = ApplicationServiceContext.Current.GetService<IRepositoryService<Patient>>();
            var cacheService = ApplicationServiceContext.Current.GetService<IAdhocCacheService>();

            Expression<Func<EntityRelationship, bool>> queryExpression = c => c.ObsoleteVersionSequenceId == null;

            // add the match result filter
            if (string.IsNullOrEmpty(matchResult))
            {
                queryExpression = c => (c.RelationshipTypeKey == MdmConstants.MasterRecordRelationship ||
                                                    c.RelationshipTypeKey == MdmConstants.CandidateLocalRelationship ||
                                                    c.RelationshipTypeKey == MdmConstants.IgnoreCandidateRelationship) && c.ObsoleteVersionSequenceId == null;
            }
            else if (!string.IsNullOrEmpty(matchResult) && matchResultMap.TryGetValue(matchResult, out var matchResultKey))
            {
                var updatedExpression = Expression.MakeBinary(ExpressionType.AndAlso, queryExpression.Body,
                    Expression.MakeBinary(ExpressionType.Equal,
                        Expression.Property(Expression.Parameter(typeof(EntityRelationship)), typeof(EntityRelationship), nameof(EntityRelationship.RelationshipTypeKey)),
                        Expression.Constant(matchResultKey, typeof(Guid?))));

                queryExpression = Expression.Lambda<Func<EntityRelationship, bool>>(updatedExpression, queryExpression.Parameters);
            }

            // add the link source filter
            if (!string.IsNullOrEmpty(linkSource) && linkSourceMap.TryGetValue(linkSource, out var linkSourceKey))
            {
                var updatedExpression = Expression.MakeBinary(ExpressionType.AndAlso, queryExpression.Body,
                    Expression.MakeBinary(ExpressionType.Equal,
                        Expression.Property(Expression.Parameter(typeof(EntityRelationship)), typeof(EntityRelationship), nameof(EntityRelationship.ClassificationKey)),
                        Expression.Constant(linkSourceKey, typeof(Guid?))));

                queryExpression = Expression.Lambda<Func<EntityRelationship, bool>>(updatedExpression, queryExpression.Parameters);
            }

            // add the target key filter
            if (Guid.TryParse(masterResourceId, out var targetKey))
            {
                var updatedExpression = Expression.MakeBinary(ExpressionType.AndAlso, queryExpression.Body,
                    Expression.MakeBinary(ExpressionType.Equal,
                        Expression.Property(Expression.Parameter(typeof(EntityRelationship)), typeof(EntityRelationship), nameof(EntityRelationship.TargetEntityKey)),
                        Expression.Constant(targetKey, typeof(Guid?))));

                queryExpression = Expression.Lambda<Func<EntityRelationship, bool>>(updatedExpression, queryExpression.Parameters);
            }

            // add the source key filter
            if (Guid.TryParse(resourceId, out var sourceKey))
            {
                var updatedExpression = Expression.MakeBinary(ExpressionType.AndAlso, queryExpression.Body,
                    Expression.MakeBinary(ExpressionType.Equal,
                        Expression.Property(Expression.Parameter(typeof(EntityRelationship)), typeof(EntityRelationship), nameof(EntityRelationship.SourceEntityKey)),
                        Expression.Constant(sourceKey, typeof(Guid?))));

                queryExpression = Expression.Lambda<Func<EntityRelationship, bool>>(updatedExpression, queryExpression.Parameters);
            }

            // query the underlying service
            var relationships = entityRelationshipService.Find(queryExpression);
            var totalResults = relationships.Count(); // send a COUNT to the db

            var resource = new Parameters();

            var previousOffset = (int)offset - (int)count < 0 ? 0 : (int)offset - (int)count;

            var builder = new StringBuilder();

            // rebuild the outgoing query string
            foreach (var key in RestOperationContext.Current.IncomingRequest.QueryString.AllKeys.Intersect(this.Parameters.Keys).Where(c => c != "_count" && c != "_offset"))
            {
                builder.Append($"&{key}={RestOperationContext.Current.IncomingRequest.QueryString[key]}");
            }

            var queryParameters = builder.ToString();

            if (offset > 0 && totalResults > 0)
            {
                resource.Parameter.Add(new Parameters.ParameterComponent
                {
                    Name = "prev",
                    Value = new FhirUri($"{RestOperationContext.Current.IncomingRequest.Url.GetLeftPart(UriPartial.Authority)}{RestOperationContext.Current.IncomingRequest.Url.LocalPath}?_offset={previousOffset}&_count={count}{queryParameters}")
                });
            }

            resource.Parameter.Add(new Parameters.ParameterComponent
            {
                Name = "self",
                Value = new FhirUri(RestOperationContext.Current.IncomingRequest.Url)
            });

            if (offset + count < totalResults)
            {
                resource.Parameter.Add(new Parameters.ParameterComponent
                {
                    Name = "next",
                    Value = new FhirUri($"{RestOperationContext.Current.IncomingRequest.Url.GetLeftPart(UriPartial.Authority)}{RestOperationContext.Current.IncomingRequest.Url.LocalPath}?_offset={offset + count}&_count={count}{queryParameters}")
                });
            }

            // build the result set
            foreach (var entityRelationship in relationships)
            {
                var patient = entityRelationship.TargetEntity as Patient ?? cacheService?.Get<Patient>(entityRelationship.TargetEntityKey.ToString());

                if (patient == null)
                {
                    patient = patientService.Get(entityRelationship.TargetEntityKey.Value);
                }

                cacheService?.Add(entityRelationship.TargetEntityKey.ToString(), patient);

                var classificationResult = matchingService.Classify(patient, new List<Patient>
                {
                    new Patient
                    {
                        Key = entityRelationship.SourceEntityKey
                    }
                }, configuration).FirstOrDefault();

                if (classificationResult == null)
                {
                    continue;
                }

                resource.Parameter.Add(new Parameters.ParameterComponent
                {
                    Name = "link",
                    Part = new List<Parameters.ParameterComponent>
                    {
                        new Parameters.ParameterComponent
                        {
                            Name = "goldenResourceId",
                            Value = new FhirString($"Patient/{entityRelationship.TargetEntityKey}")
                        },
                        new Parameters.ParameterComponent
                        {
                            Name = "sourceResourceId",
                            Value = new FhirString($"Patient/{classificationResult.Record.Key}")
                        },
                        new Parameters.ParameterComponent
                        {
                            Name = "matchResult",
                            Value = new FhirString(matchResultMap.First(c => c.Value == entityRelationship.RelationshipTypeKey).Key)
                        },
                        new Parameters.ParameterComponent
                        {
                            Name = "linkSource",
                            Value = new FhirString(linkSourceMap.First(c => c.Value == entityRelationship.ClassificationKey).Key)
                        },
                        new Parameters.ParameterComponent
                        {
                            Name = "score",
                            Value = new FhirDecimal(Convert.ToDecimal(classificationResult.Score))
                        }
                    }
                });
            }

            resource.Parameter.Add(new Parameters.ParameterComponent
            {
                Name = "resultCount",
                Value = new Integer(resource.Parameter.Count(c => c.Name == "link"))
            });

            resource.Parameter.Add(new Parameters.ParameterComponent
            {
                Name = "totalResults",
                Value = new Integer(totalResults)
            });

            return resource;
        }
    }
}