using System;
using System.Collections.Generic;
using System.Text;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Event;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Patch;
using SanteDB.Core.Services;

namespace SanteDB.Persistence.MDM.Services.Resources
{
    /// <summary>
    /// An implementation of a IRecordMergeService for an MDM controlled resource
    /// </summary>
    public abstract class MdmResourceMerger<TModel> : IRecordMergingService<TModel>
        where TModel : IdentifiedData, new()
    {

        // Get specified tracer
        protected Tracer m_tracer = Tracer.GetTracer(typeof(MdmResourceMerger<TModel>));

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
        public abstract IEnumerable<Guid> GetIgnoreList(Guid masterKey);

        /// <summary>
        /// Get the merge candidate keys
        /// </summary>
        public abstract IEnumerable<Guid> GetMergeCandidates(Guid masterKey);

        /// <summary>
        /// Ignore the specified candidate
        /// </summary>
        public abstract void Ignore(Guid masterKey, IEnumerable<Guid> falsePositives);

        /// <summary>
        /// Merge the specified duplicates
        /// </summary>
        public abstract void Merge(Guid masterKey, IEnumerable<Guid> linkedDuplicates);

        /// <summary>
        /// Un-ignore the specified object
        /// </summary>
        public abstract void UnIgnore(Guid masterKey, IEnumerable<Guid> ignoredKeys);

        /// <summary>
        /// Un-merge the specified object
        /// </summary>
        public abstract void Unmerge(Guid masterKey, Guid unmergeDuplicateKey);


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
    }
}
