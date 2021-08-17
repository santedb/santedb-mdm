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

            if(me.ClassConceptKey == MdmConstants.MasterRecordClassification && entityTypeMap.TryGetValue(me.TypeConceptKey.Value, out Type tMaster))
            {
                var emaster = Activator.CreateInstance(typeof(EntityMaster<>).MakeGenericType(tMaster), me) as IMdmMaster;
                return emaster.GetMaster(AuthenticationContext.Current.Principal) as Entity;
            }
            else
            {
                return me;
            }

        }

    }
}
