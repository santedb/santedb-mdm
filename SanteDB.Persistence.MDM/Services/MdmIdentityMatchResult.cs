/*
 * Copyright (C) 2021 - 2026, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2023-6-21
 */
using SanteDB.Core.Matching;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace SanteDB.Persistence.MDM.Services
{

    /// <summary>
    /// Identity match attribute
    /// </summary>
    internal class MdmIdentityMatchAttribute : IRecordMatchVector
    {

        /// <summary>
        /// Create MDM match attribute
        /// </summary>
        public MdmIdentityMatchAttribute(RecordMatchClassification classification, object aValue, object bValue)
        {
            this.Score = classification == RecordMatchClassification.Match ? 1.0f : 0.0f;
            this.A = aValue;
            this.B = bValue;
        }

        /// <summary>
        /// Gets the name
        /// </summary>
        public string Name => nameof(Entity.Identifiers);

        /// <summary>
        /// Gets the score
        /// </summary>
        public double Score { get; }

        /// <summary>
        /// Get the A value
        /// </summary>
        public object A { get; }

        /// <summary>
        /// Get the B value
        /// </summary>
        public object B { get; }

        /// <summary>
        /// Evaluated
        /// </summary>
        public bool Evaluated => true;
    }

    /// <summary>
    /// Represents an internal MDM match result based on a unique ID
    /// </summary>
    internal class MdmIdentityMatchResult<T> : IRecordMatchResult<T> where T : IdentifiedData
    {

        /// <summary>
        /// Get the configuration name
        /// </summary>
        public IRecordMatchingConfiguration Configuration { get; }

        /// <summary>
        /// Gets the record that was matched
        /// </summary>
        public T Record { get; }

        /// <summary>
        /// Gets the score of the record
        /// </summary>
        public double Score { get; }

        /// <summary>
        /// Gets the strength of the match
        /// </summary>
        public double Strength { get; }

        /// <summary>
        /// Gets the classification 
        /// </summary>
        public RecordMatchClassification Classification { get; }

        /// <summary>
        /// Gets the method of match
        /// </summary>
        public RecordMatchMethod Method { get; }

        /// <summary>
        /// Gets the record that was matched
        /// </summary>
        IdentifiedData IRecordMatchResult.Record => this.Record;

        /// <summary>
        /// Get the attributes for this match result
        /// </summary>
        public IEnumerable<IRecordMatchVector> Vectors { get; }

        /// <summary>
        /// Create a new identity match result
        /// </summary>
        public MdmIdentityMatchResult(T input, T record, RecordMatchClassification classification = RecordMatchClassification.Match, float score = 1.0f)
        {
            this.Record = record;
            this.Method = RecordMatchMethod.Identifier;
            this.Score = this.Strength = score;
            this.Classification = classification;
            this.Configuration = MdmMatchConfigurationService.CreateIdentityMatchConfiguration<T>();
            if (input is IHasIdentifiers aIdentity && record is IHasIdentifiers bIdentity)
            {
                this.Vectors = new IRecordMatchVector[] { new MdmIdentityMatchAttribute(classification, string.Join(",", aIdentity.Identifiers.Select(o => $"{o.Value} [{o.IdentityDomain.DomainName}]")), string.Join(",", bIdentity.Identifiers.Select(o => $"{o.Value} [{o.IdentityDomain.DomainName}]"))) };
            }
        }


    }
}