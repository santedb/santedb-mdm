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
 * Date: 2021-10-29
 */
using SanteDB.Core.Matching;
using System;
using System.Collections.Generic;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Represents a master match 
    /// </summary>
    public class MasterMatch : IEqualityComparer<MasterMatch>, IEquatable<MasterMatch>
    {
        /// <summary>
        /// Gets the master UUID
        /// </summary>
        public Guid Master { get; set; }

        /// <summary>
        /// Gets the match result
        /// </summary>
        public IRecordMatchResult MatchResult { get; set; }
        
        /// <summary>
        /// Creates a new master match
        /// </summary>
        public MasterMatch(Guid master, IRecordMatchResult match)
        {
            this.MatchResult = match;
            this.Master = master;
        }

        /// <summary>
        /// Determine if x equals y
        /// </summary>
        public bool Equals(MasterMatch x, MasterMatch y)
        {
            return x.Master == y.Master;
        }

        /// <summary>
        /// Get hash code
        /// </summary>
        public int GetHashCode(MasterMatch obj)
        {
            return obj.Master.GetHashCode();
        }

        /// <summary>
        /// True if this equals other
        /// </summary>
        public bool Equals(MasterMatch other)
        {
            return this.Master == other.Master;
        }
    }

}