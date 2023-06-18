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
 * Date: 2023-5-19
 */
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Services;
using System;
using System.Collections.Generic;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// An implementation of a IRecordMergeService for an MDM controlled resource
    /// </summary>
    public abstract class MdmResourceMerger<TModel> : IRecordMergingService<TModel>
        where TModel : IdentifiedData, new()
    {
        // Get specified tracer
        protected readonly Tracer m_tracer = Tracer.GetTracer(typeof(MdmResourceMerger<TModel>));

        /// <summary>
        /// Name of the merging service
        /// </summary>
        public string ServiceName => $"MDM Record Linker/Merger for {typeof(TModel).Name}";

        /// <summary>
        /// Fired when the merging is occurring
        /// </summary>
        public event EventHandler<DataMergingEventArgs<TModel>> Merging;

        /// <summary>
        /// Fired after the merge is complete
        /// </summary>
        public event EventHandler<DataMergeEventArgs<TModel>> Merged;

        /// <summary>
        /// Fired when un-merging is going to occur
        /// </summary>
        public event EventHandler<DataMergingEventArgs<TModel>> UnMerging;

        /// <summary>
        /// Fired after un-merge is complete
        /// </summary>
        public event EventHandler<DataMergeEventArgs<TModel>> UnMerged;

        /// <summary>
        /// Get the ignore list
        /// </summary>
        public abstract IEnumerable<Guid> GetIgnoredKeys(Guid masterKey);

        /// <summary>
        /// Get the ignore list
        /// </summary>
        public abstract IQueryResultSet<IdentifiedData> GetIgnored(Guid masterKey);

        /// <summary>
        /// Get the merge candidate keys
        /// </summary>
        public abstract IEnumerable<Guid> GetMergeCandidateKeys(Guid masterKey);

        /// <summary>
        /// Get merge candidates
        /// </summary>
        public abstract IQueryResultSet<IdentifiedData> GetMergeCandidates(Guid masterKey);

        /// <summary>
        /// Ignore the specified candidate
        /// </summary>
        public abstract IdentifiedData Ignore(Guid masterKey, IEnumerable<Guid> falsePositives);

        /// <summary>
        /// Merge the specified duplicates
        /// </summary>
        public abstract RecordMergeResult Merge(Guid masterKey, IEnumerable<Guid> linkedDuplicates);

        /// <summary>
        /// Un-ignore the specified object
        /// </summary>
        public abstract IdentifiedData UnIgnore(Guid masterKey, IEnumerable<Guid> ignoredKeys);

        /// <summary>
        /// Un-merge the specified object
        /// </summary>
        public abstract RecordMergeResult Unmerge(Guid masterKey, Guid unmergeDuplicateKey);

        /// <summary>
        /// Fire mergingin event  returning whether the merge shoudl be cancelled
        /// </summary>
        protected bool FireMerging(Guid surviorKey, IEnumerable<Guid> linkedKeys)
        {
            var dpe = new DataMergingEventArgs<TModel>(surviorKey, linkedKeys);
            this.Merging?.Invoke(this, dpe);
            return dpe.Cancel;
        }

        /// <summary>
        /// Fire mergingin event  returning whether the merge shoudl be cancelled
        /// </summary>
        protected void FireMerged(Guid surviorKey, IEnumerable<Guid> linkedKeys)
        {
            var dpe = new DataMergeEventArgs<TModel>(surviorKey, linkedKeys);
            this.Merged?.Invoke(this, dpe);
        }

        /// <summary>
        /// Fire mergingin event  returning whether the merge shoudl be cancelled
        /// </summary>
        protected bool FireUnmerging(Guid surviorKey, IEnumerable<Guid> linkedKeys)
        {
            var dpe = new DataMergingEventArgs<TModel>(surviorKey, linkedKeys);
            this.UnMerging?.Invoke(this, dpe);
            return dpe.Cancel;
        }

        /// <summary>
        /// Fire mergingin event  returning whether the merge shoudl be cancelled
        /// </summary>
        protected void FireUnmerged(Guid surviorKey, IEnumerable<Guid> linkedKeys)
        {
            var dpe = new DataMergeEventArgs<TModel>(surviorKey, linkedKeys);
            this.UnMerged?.Invoke(this, dpe);
        }

        /// <summary>
        /// Get all global merge candidates
        /// </summary>
        public abstract IQueryResultSet<ITargetedAssociation> GetGlobalMergeCandidates();

        /// <summary>
        /// Detect global merge candidates
        /// </summary>
        public abstract void DetectGlobalMergeCandidates();

        /// <summary>
        /// Clear global merge candidates
        /// </summary>
        public abstract void ClearGlobalMergeCanadidates();

        /// <summary>
        /// Clear global ignore flags
        /// </summary>
        public abstract void ClearGlobalIgnoreFlags();

        /// <summary>
        /// Reset the global merge candidates, MDM links, etc.
        /// </summary>
        public abstract void Reset(bool includeVerified, bool linksOnly);

        /// <summary>
        /// Reset the specified master key of all matching information
        /// </summary>
        public abstract void Reset(Guid masterKey, bool includeVerified, bool linksOnly);

        /// <summary>
        /// Clear merge candidates
        /// </summary>
        public abstract void ClearMergeCandidates(Guid masterKey);

        /// <summary>
        /// Clear ignore keys
        /// </summary>
        public abstract void ClearIgnoreFlags(Guid masterKey);
    }
}