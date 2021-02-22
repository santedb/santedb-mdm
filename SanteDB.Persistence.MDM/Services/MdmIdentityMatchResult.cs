/*
 * Portions Copyright (C) 2019 - 2020, Fyfe Software Inc. and the SanteSuite Contributors (See NOTICE.md)
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
 * Date: 2020-2-2
 */
using SanteDB.Core.Model;
using SanteDB.Core.Services;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Represents an internal MDM match result based on a unique ID
    /// </summary>
    internal class MdmIdentityMatchResult<T> : IRecordMatchResult<T> where T : IdentifiedData
    {

        /// <summary>
        /// Gets the record that was matched
        /// </summary>
        public T Record { get; }

        /// <summary>
        /// Gets the score of the record
        /// </summary>
        public double Score { get; }

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
        /// Create a new identity match result
        /// </summary>
        public MdmIdentityMatchResult(T record)
        {
            this.Record = record;
            this.Method = RecordMatchMethod.Identifier;
            this.Score = 1.0f;
            this.Classification = RecordMatchClassification.Match;
        }
    }
}