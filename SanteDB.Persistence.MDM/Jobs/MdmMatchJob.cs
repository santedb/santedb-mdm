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
using SanteDB.Core;
using SanteDB.Core.Diagnostics;
using SanteDB.Core.Jobs;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Acts;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Query;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Services.Resources;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Jobs
{
    /// <summary>
    /// Represents a match job
    /// </summary>
    /// <typeparam name="T">The type of object to match on</typeparam>
    [DisplayName("MDM Batch Matching Job")]
    [ExcludeFromCodeCoverage]
    public class MdmMatchJob<T> : IReportProgressJob
        where T : IdentifiedData, new()
    {
        // Guid
        private Guid m_id = Guid.NewGuid();

        // Merge service
        private IRecordMergingService<T> m_mergeService;

        // Tracer
        private readonly Tracer m_tracer = Tracer.GetTracer(typeof(MdmMatchJob<T>));

        /// <summary>
        /// Create a match job
        /// </summary>
        public MdmMatchJob(IRecordMergingService<T> recordMergingService)
        {
            this.m_mergeService = recordMergingService;
            // Progress change handler
            if (this.m_mergeService is IReportProgressChanged rpt)
            {
                rpt.ProgressChanged += (o, p) =>
                {
                    this.Progress = p.Progress;
                    this.StatusText = p.State.ToString();
                };
            }
        }

        /// <summary>
        /// Get the identifier
        /// </summary>
        public Guid Id => this.m_id;

        /// <summary>
        /// Name of the matching job
        /// </summary>
        public string Name => $"Background Matching Job for {typeof(T).Name}";


        /// <inheritdoc/>
        public string Description => $"Starts a background process which re-processes detected duplicate SOURCE records for {typeof(T).Name}";

        /// <summary>
        /// Can cancel the job?
        /// </summary>
        public bool CanCancel => false;

        /// <summary>
        /// Gets the current state
        /// </summary>
        public JobStateType CurrentState { get; private set; }

        /// <summary>
        /// Gets the parameters for the job
        /// </summary>
        public IDictionary<string, Type> Parameters => new Dictionary<String, Type>()
        {
            { "clearExistingMdmData", typeof(bool) }
        };

        /// <summary>
        /// Last time the job started
        /// </summary>
        public DateTime? LastStarted { get; private set; }

        /// <summary>
        /// Last time job finished
        /// </summary>
        public DateTime? LastFinished { get; private set; }

        /// <summary>
        /// Progress
        /// </summary>
        public float Progress { get; private set; }

        /// <summary>
        /// Gets or sets the status text
        /// </summary>
        public string StatusText { get; private set; }

        /// <summary>
        /// Cancel the job
        /// </summary>
        public void Cancel()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Run the specified job
        /// </summary>
        public void Run(object sender, EventArgs e, object[] parameters)
        {
            try
            {
                using (AuthenticationContext.EnterSystemContext())
                {
                    this.LastStarted = DateTime.Now;
                    this.CurrentState = JobStateType.Running;
                    var clear = parameters.Length > 0 ? (bool?)parameters[0] : false;
                    this.m_tracer.TraceInfo("Starting batch run of MDM Matching ");
                   
                    if (clear.GetValueOrDefault())
                    {
                        this.m_tracer.TraceVerbose("Batch instruction indicates clear of all links");
                        this.m_mergeService.ClearGlobalIgnoreFlags();
                        this.m_mergeService.ClearGlobalMergeCanadidates();
                    }
                    else
                    {
                        this.m_mergeService.ClearGlobalMergeCanadidates();
                    }

                    this.m_mergeService.DetectGlobalMergeCandidates();

                    this.LastFinished = DateTime.Now;
                    this.CurrentState = JobStateType.Completed;
                }
            }
            catch (Exception ex)
            {
                this.CurrentState = JobStateType.Aborted;
                this.m_tracer.TraceError("Could not run MDM Matching Job: {0}", ex);
            }
        }
    }
}