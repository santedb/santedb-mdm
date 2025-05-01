/*
 * Copyright (C) 2021 - 2025, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
 * Portions Copyright (C) 2019 - 2021, Fyfe Software Inc. and the SanteSuite Contributors
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
using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Collection;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace SanteDB.Persistence.MDM.Test
{
    /// <summary>
    /// Business rule service
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class BundleBusinessRule : BaseBusinessRulesService<Bundle>
    {
        /// <summary>
        /// Fire before the insert of an object
        /// </summary>
        public override Bundle BeforeInsert(Bundle data)
        {
            for (int i = 0; i < data.Item.Count; i++)
            {
                data.Item[i] = ApplicationServiceContext.Current.GetBusinessRuleService(data.Item[i].GetType())?.BeforeInsert(data.Item[i]) as IdentifiedData ?? data.Item[i];
            }

            return base.BeforeInsert(data);
        }

        /// <summary>
        /// Fire before the insert of an object
        /// </summary>
        public override Bundle BeforeUpdate(Bundle data)
        {
            for (int i = 0; i < data.Item.Count; i++)
            {
                data.Item[i] = ApplicationServiceContext.Current.GetBusinessRuleService(data.Item[i].GetType())?.BeforeUpdate(data.Item[i]) as IdentifiedData ?? data.Item[i];
            }

            return base.BeforeUpdate(data);
        }

    }

    /// <summary>
    /// National health identifier BRE
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class NationalHealthIdRule : BaseBusinessRulesService<EntityMaster<Patient>>
    {

        private static readonly Guid m_aaid = Guid.NewGuid();

        public static int LastGeneratedNhid = 0;

        /// <summary>
        /// Before record is inserted
        /// </summary>
        public override EntityMaster<Patient> BeforeInsert(EntityMaster<Patient> data)
        {
            return this.DoAttach(data);
        }

        /// <summary>
        /// Before update 
        /// </summary>
        public override EntityMaster<Patient> BeforeUpdate(EntityMaster<Patient> data)
        {
            return this.DoAttach(data);
        }

        /// <summary>
        /// Do an attachment
        /// </summary>
        private EntityMaster<Patient> DoAttach(EntityMaster<Patient> data)
        {
            if (!data.LoadProperty(o => o.Identifiers).Any(o => o.LoadProperty(i => i.IdentityDomain).DomainName == "NHID"))
            {
                data.Identifiers.Add(new Core.Model.DataTypes.EntityIdentifier(new IdentityDomain("NHID", "NHID", "3.2.2.3.2.2.3.2")
                {
                    Key = m_aaid,
                    AuthorityScopeXml = new List<Guid>() { MdmConstants.MasterRecordClassification }
                }, (++LastGeneratedNhid).ToString()));
            }

            return base.BeforeInsert(data);
        }
    }
}
