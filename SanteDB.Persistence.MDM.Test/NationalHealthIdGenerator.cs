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
 * Date: 2020-2-4
 */
using SanteDB.Core;
using SanteDB.Core.BusinessRules;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Services;
using SanteDB.Core.Services.Impl;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SanteDB.Persistence.MDM.Test
{
    /// <summary>
    /// Business rule service
    /// </summary>
    public class BundleBusinessRule : BaseBusinessRulesService<Bundle>
    {
        /// <summary>
        /// Fire before the insert of an object
        /// </summary>
        public override Bundle BeforeInsert(Bundle data)
        {
            for (int i = 0; i < data.Item.Count; i++)
                data.Item[i] = ApplicationServiceContext.Current.GetBusinessRuleService(data.Item[i].GetType())?.BeforeInsert(data.Item[i]) as IdentifiedData ?? data.Item[i];
            return base.BeforeInsert(data);
        }
    }

    /// <summary>
    /// National health identifier BRE
    /// </summary>
    public class NationalHealthIdRule : BaseBusinessRulesService<EntityMaster<Patient>>
    {

        public static int LastGeneratedNhid = 0;

        /// <summary>
        /// Before record is inserted
        /// </summary>
        public override EntityMaster<Patient> BeforeInsert(EntityMaster<Patient> data)
        {
            if (!data.Identifiers.Any(o => o.Authority.DomainName == "NHID"))
                data.Identifiers.Add(new Core.Model.DataTypes.EntityIdentifier(new AssigningAuthority("NHID", "NHID", "3.2.2.3.2.2.3.2")
                {
                    AuthorityScopeXml = new List<Guid>() { MdmConstants.MasterRecordClassification }
                }, (++LastGeneratedNhid).ToString()));
            return base.BeforeInsert(data);
        }
    }
}
