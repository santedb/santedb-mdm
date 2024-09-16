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
using NUnit.Framework;
using SanteDB.Core;
using SanteDB.Core.Matching;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace SanteDB.Persistence.MDM.Test
{
    /// <summary>
    /// Implements a matcher service which only matches date of birth
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class DummyMatcherService : IRecordMatchingService, IRecordMatchingConfigurationService
    {
        /// <summary>
        /// Gets the service name
        /// </summary>
        public string ServiceName => "";

        /// <summary>
        /// Get all configurations
        /// </summary>
        public IEnumerable<IRecordMatchingConfiguration> Configurations
        {
            get
            {
                yield return new DummyMatchConfiguration();
            }
        }

        /// <summary>
        /// Perform blocking
        /// </summary>
        public IQueryResultSet<T> Block<T>(T input, string configurationName, IEnumerable<Guid> ignoreList, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            if (input.GetType() == typeof(Patient))
            {
                Patient p = (Patient)((Object)input);
                return ApplicationServiceContext.Current.GetService<IDataPersistenceService<Patient>>().Query(o => o.DateOfBirth == p.DateOfBirth && o.Key != p.Key, AuthenticationContext.Current.Principal).TransformResultSet<Patient, T>();
            }
            return new MemoryQueryResultSet<T>(new List<T>());
        }

        /// <summary>
        /// Classify the patient records
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Classify<T>(T input, IEnumerable<T> blocks, string configurationName, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            return blocks.Select(o => new DummyMatchResult<T>(input, o));
        }

        /// <summary>
        /// Match existing records with others
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Match<T>(T input, string configurationName, IEnumerable<Guid> ignoreList, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            Assert.AreEqual("default", configurationName);
            return this.Classify(input, this.Block(input, configurationName, ignoreList, collector), configurationName, collector);
        }

        /// <summary>
        /// Match
        /// </summary>
        public IEnumerable<IRecordMatchResult> Match(IdentifiedData input, string configurationName, IEnumerable<Guid> ignoreList, IRecordMatchingDiagnosticSession collector = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Classify
        /// </summary>
        public IEnumerable<IRecordMatchResult> Classify(IdentifiedData input, IEnumerable<IdentifiedData> blocks, String configurationName, IRecordMatchingDiagnosticSession collector = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Perform a score
        /// </summary>
        public IRecordMatchResult<T> Score<T>(T input, Expression<Func<T, bool>> query, string configurationName, IRecordMatchingDiagnosticSession collector = null) where T : IdentifiedData
        {
            if (input.GetType() == typeof(Patient))
            {
                Patient p = (Patient)((Object)input);
                return new DummyMatchResult<T>(input, input);
            }
            else
            {
                return null;
            }
        }

        public IRecordMatchingDiagnosticSession CreateDiagnosticSession()
        {
            throw new NotImplementedException();
        }

        public IRecordMatchingConfiguration GetConfiguration(string configurationId)
        {
            throw new NotImplementedException();
        }

        public IRecordMatchingConfiguration SaveConfiguration(IRecordMatchingConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        public IRecordMatchingConfiguration DeleteConfiguration(string configurationId)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Dummy match configuration
    /// </summary>
    [ExcludeFromCodeCoverage]
    internal class DummyMatchConfiguration : IRecordMatchingConfiguration
    {
        public DummyMatchConfiguration()
        {
            this.Metadata = new DummyMatchConfigurationMetadata();
        }

        /// <summary>
        /// UUID
        /// </summary>
        public Guid Uuid => Guid.NewGuid();

        /// <summary>
        /// Identifier
        /// </summary>
        public string Id => "default";

        /// <summary>
        /// Applies to patient
        /// </summary>
        public Type[] AppliesTo => new Type[] { typeof(Patient) };

        /// <summary>
        /// Get the metadata
        /// </summary>
        public IRecordMatchingConfigurationMetadata Metadata
        {
            get; set;
        }

        /// <summary>
        /// Match configuration metadata
        /// </summary>
        private class DummyMatchConfigurationMetadata : IRecordMatchingConfigurationMetadata
        {
            /// <summary>
            /// Created by
            /// </summary>
            public string CreatedBy => "SYSTEM";

            /// <summary>
            /// Creation time
            /// </summary>
            public DateTimeOffset CreationTime => DateTimeOffset.Now;

            /// <summary>
            /// State of the configuration
            /// </summary>
            public MatchConfigurationStatus Status => MatchConfigurationStatus.Active;

            /// <summary>
            /// Is readonly
            /// </summary>
            public bool IsReadonly => true;

            /// <summary>
            /// Get all tags
            /// </summary>
            public IDictionary<string, string> Tags => new Dictionary<String, String>()
            {
                { MdmConstants.AutoLinkSetting, "true" }
            };

            /// <summary>
            /// Updated time
            /// </summary>
            public DateTimeOffset? UpdatedTime => null;

            /// <summary>
            /// Updated by
            /// </summary>
            public string UpdatedBy => null;
        }
    }

    /// <summary>
    /// Represent a dummy match result
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class DummyMatchResult<T> : IRecordMatchResult<T>
        where T : IdentifiedData
    {
        // The record
        private T m_record;

        /// <summary>
        /// Get the score
        /// </summary>
        public double Score => 1.0;

        /// <summary>
        /// Strength of the match
        /// </summary>
        public double Strength => 1.0;

        /// <summary>
        /// Gets the matching record
        /// </summary>
        public T Record => this.m_record;

        /// <summary>
        /// Match classification
        /// </summary>
        public RecordMatchClassification Classification { get; private set; }

        /// <summary>
        /// Return the record
        /// </summary>
        IdentifiedData IRecordMatchResult.Record => this.m_record;

        /// <summary>
        /// Gets the method
        /// </summary>
        public RecordMatchMethod Method => RecordMatchMethod.Weighted;

        public IEnumerable<IRecordMatchVector> Vectors => throw new NotImplementedException();

        public IRecordMatchingConfiguration Configuration => new DummyMatchConfiguration();

        /// <summary>
        /// Create a dummy match
        /// </summary>
        public DummyMatchResult(T input, T record)
        {
            this.m_record = record;

            // Patient?
            if (input is Patient)
            {
                var pInput = (Patient)(object)input;
                var pRecord = (Patient)(object)record;
                // Classify
                if (pInput.MultipleBirthOrder.HasValue && pInput.MultipleBirthOrder != pRecord.MultipleBirthOrder)
                {
                    this.Classification = RecordMatchClassification.Probable;
                }
                else
                {
                    this.Classification = RecordMatchClassification.Match;
                }
            }
            else
            {
                this.Classification = RecordMatchClassification.Match;
            }
        }
    }
}