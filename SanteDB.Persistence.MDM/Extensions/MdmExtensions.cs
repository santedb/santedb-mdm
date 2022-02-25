/*
 * Copyright (C) 2021 - 2021, SanteSuite Inc. and the SanteSuite Contributors (See NOTICE.md for full copyright notices)
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
 * Date: 2021-8-22
 */
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security;
using SanteDB.Persistence.MDM.Model;
using SanteDB.Persistence.MDM.Services.Resources;
using System;
using System.Collections.Generic;
using System.Text;

namespace SanteDB.Persistence.MDM.Extensions
{
    /// <summary>
    /// MDM extension methods
    /// </summary>
    public static class MdmExtensions
    {

        private static readonly Dictionary<Guid, Type> entityTypeMap = new Dictionary<Guid, Type>() {
            { EntityClassKeys.Patient, typeof(Patient) },
            { EntityClassKeys.Provider, typeof(Provider) },
            { EntityClassKeys.Organization, typeof(Organization) },
            { EntityClassKeys.Place, typeof(Place) },
            { EntityClassKeys.CityOrTown, typeof(Place) },
            { EntityClassKeys.Country, typeof(Place) },
            { EntityClassKeys.CountyOrParish, typeof(Place) },
            { EntityClassKeys.State, typeof(Place) },
            { EntityClassKeys.PrecinctOrBorough, typeof(Place) },
            { EntityClassKeys.ServiceDeliveryLocation, typeof(Place) },
            { EntityClassKeys.Person, typeof(Person) },
            { EntityClassKeys.ManufacturedMaterial, typeof(ManufacturedMaterial) },
            { EntityClassKeys.Material, typeof(Material) }
        };

        /// <summary>
        /// Get master for <paramref name="me"/> or, if it is already a master or not MDM controlled return <paramref name="me"/>
        /// </summary>
        public static Entity GetMaster(this Entity me)
        {
            if (me == null)
            {
                return null;
            }
            if (me.ClassConceptKey == MdmConstants.MasterRecordClassification && entityTypeMap.TryGetValue(me.TypeConceptKey.Value, out Type tMaster))
            {
                var emaster = Activator.CreateInstance(typeof(EntityMaster<>).MakeGenericType(tMaster), me) as IMdmMaster;
                return emaster.Synthesize(AuthenticationContext.Current.Principal) as Entity;
            }
            else
            {
                return me;
            }

        }

    }
}