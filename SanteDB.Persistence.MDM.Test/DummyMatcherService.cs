using NUnit.Framework;
using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Core.Matching;
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
        public IEnumerable<T> Block<T>(T input, string configurationName, IEnumerable<Guid> ignoreList) where T : IdentifiedData
        {
            if (input.GetType() == typeof(Patient))
            {
                Patient p = (Patient)((Object)input);
                return ApplicationServiceContext.Current.GetService<IDataPersistenceService<Patient>>().Query(o => o.DateOfBirth == p.DateOfBirth && o.Key != p.Key, AuthenticationContext.Current.Principal).OfType<T>();
            }
            return new List<T>();
        }

        /// <summary>
        /// Classify the patient records
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Classify<T>(T input, IEnumerable<T> blocks, string configurationName) where T : IdentifiedData
        {
            return blocks.Select(o => new DummyMatchResult<T>(input, o));
        }

        /// <summary>
        /// Match existing records with others
        /// </summary>
        public IEnumerable<IRecordMatchResult<T>> Match<T>(T input, string configurationName, IEnumerable<Guid> ignoreList) where T : IdentifiedData
        {
            Assert.AreEqual("default", configurationName);
            return this.Classify(input, this.Block(input, configurationName, ignoreList), configurationName);
        }

        /// <summary>
        /// Match
        /// </summary>
        public IEnumerable<IRecordMatchResult> Match(IdentifiedData input, string configurationName, IEnumerable<Guid> ignoreList)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Classify
        /// </summary>
        public IEnumerable<IRecordMatchResult> Classify(IdentifiedData input, IEnumerable<IdentifiedData> blocks, String configurationName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Perform a score
        /// </summary>
        public IRecordMatchResult<T> Score<T>(T input, Expression<Func<T, bool>> query, string configurationName) where T : IdentifiedData
        {
            if (input.GetType() == typeof(Patient))
            {
                Patient p = (Patient)((Object)input);
                return new DummyMatchResult<T>(input, input);
            }
            else return null;
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
            public MatchConfigurationStatus State => MatchConfigurationStatus.Active;

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

        public string ConfigurationName => throw new NotImplementedException();

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
                    this.Classification = RecordMatchClassification.Probable;
                else
                    this.Classification = RecordMatchClassification.Match;
            }
            else
                this.Classification = RecordMatchClassification.Match;
        }
    }
}