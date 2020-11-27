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
 * Date: 2020-10-2
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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Jobs
{
    /// <summary>
    /// Represents a match job
    /// </summary>
    /// <typeparam name="T">The type of object to match on</typeparam>
    public class MdmMatchJob<T> : IReportProgressJob
        where T: IdentifiedData, new()
    {

        // Cancel requested
        private bool m_cancelRequest = false;

        // Tracer
        private Tracer m_tracer = Tracer.GetTracer(typeof(MdmMatchJob<T>));

        /// <summary>
        /// Name of the matching job
        /// </summary>
        public string Name => $"Background Matching Job for {typeof(T).Name}";

        /// <summary>
        /// Can cancel the job?
        /// </summary>
        public bool CanCancel => true;

        /// <summary>
        /// Gets the current state
        /// </summary>
        public JobStateType CurrentState { get; private set; }

        /// <summary>
        /// Gets the parameters for the job
        /// </summary>
        public IDictionary<string, Type> Parameters => new Dictionary<String, Type>()
        {
            { "configurationName", typeof(String) }
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
        /// Cancel the job
        /// </summary>
        public void Cancel()
        {
            this.m_cancelRequest = true;
        }

        /// <summary>
        /// Run the specified job
        /// </summary>
        public void Run(object sender, EventArgs e, object[] parameters)
        {
            try
            {
                this.LastStarted = DateTime.Now;
                this.CurrentState = JobStateType.Running;

                var configName = parameters.Length == 0 ? null : parameters[0].ToString();
                var persistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<T>>() as IStoredQueryDataPersistenceService<T>;
                var mergeService = ApplicationServiceContext.Current.GetService<IRecordMergingService<T>>();
                var queryService = ApplicationServiceContext.Current.GetService<IQueryPersistenceService>(); 

                this.m_tracer.TraceInfo("Starting batch run of MDM Matching using configuration {0}", configName);
                
                // Fetch all then run
                var queryId = Guid.NewGuid();
                int ofs = 0;
                Expression<Func<T, bool>> query = null;

                if (typeof(Act).IsAssignableFrom(typeof(T)))
                    query = QueryExpressionParser.BuildLinqExpression<T>(new NameValueCollection()
                    {
                        {
                            "statusConcept" , StatusKeyStrings.Completed
                        }
                    });
                else
                    query = QueryExpressionParser.BuildLinqExpression<T>(new NameValueCollection()
                    {
                        {
                            "statusConcept" , StatusKeyStrings.Active
                        }
                    });

                var results = persistenceService.Query(o => true, queryId, ofs, 1, out int totalCount, AuthenticationContext.SystemPrincipal);

                // Fetch all keys
                var keys = queryService.GetQueryResults(queryId, 0, 20);
                while(ofs < totalCount)
                {
                    // Process results
                    keys.AsParallel().WithDegreeOfParallelism(2).ForAll(itm =>
                    {
                        this.m_tracer.TraceVerbose("Running match on {0}", itm);
                        try
                        {
                            mergeService.FlagDuplicates(itm, configName);
                        }
                        catch (Exception ex)
                        {
                            this.m_tracer.TraceWarning("Couldn't apply match on {0} - {1}", itm, ex);
                        }
                    });

                    this.Progress = (float)ofs / (float)totalCount;
                    this.m_tracer.TraceInfo("MATCH JOB: {0:0%} COMPLETE", this.Progress);
                    ofs += 20;
                    keys = queryService.GetQueryResults(queryId, ofs, 20);
                }

                this.LastFinished = DateTime.Now;
                this.CurrentState = JobStateType.Completed;
            }
            catch(Exception ex)
            {
                this.CurrentState = JobStateType.Aborted;
                this.m_tracer.TraceError("Could not run MDM Matching Job: {0}", ex);
            }
        }
    }
}
