﻿using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.EntityLoader;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Security;
using SanteDB.Core.Services;
using SanteDB.Persistence.MDM.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SanteDB.Persistence.MDM.Services
{
    /// <summary>
    /// Entity provider which handles delay loads for MDM resources
    /// </summary>
    public class MdmEntityProvider : IEntitySourceProvider
    {

        private readonly Dictionary<Guid, Type> entityTypeMap = new Dictionary<Guid, Type>() {
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
        /// Creates a new persistence entity source
        /// </summary>
        public MdmEntityProvider()
        {
        }


        #region IEntitySourceProvider implementation

        /// <summary>
        /// Get the persistence service source
        /// </summary>
        public TObject Get<TObject>(Guid? key) where TObject : IdentifiedData, new()
        {
            return this.Get<TObject>(key, null);
        }

        /// <summary>
        /// Get the specified version
        /// </summary>
        public TObject Get<TObject>(Guid? key, Guid? versionKey) where TObject : IdentifiedData, new()
        {
            var persistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<TObject>>();
            if (persistenceService != null && key.HasValue)
            {
                var retVal = persistenceService.Get(key.Value, versionKey, true, AuthenticationContext.Current.Principal);
                if (retVal is Entity entity && entity.ClassConceptKey == MdmConstants.MasterRecordClassification)
                {
                    if (entityTypeMap.TryGetValue(entity.TypeConceptKey.GetValueOrDefault(), out Type t))
                    {
                        var master = Activator.CreateInstance(typeof(EntityMaster<>).MakeGenericType(t), entity) as IMdmMaster;
                        return (TObject)master.GetMaster(AuthenticationContext.Current.Principal);
                    }
                    else
                    {
                        return retVal;
                    }
                }
                else
                {
                    return retVal;
                }
            }

            return default(TObject);
        }

        /// <summary>
        /// Get versioned relationships for the object
        /// </summary>
        public IEnumerable<TObject> GetRelations<TObject>(Guid? sourceKey, int? sourceVersionSequence) where TObject : IdentifiedData, IVersionedAssociation, new()
        {
            // Is the collection already loaded?
            return this.Query<TObject>(o => o.SourceEntityKey == sourceKey && o.ObsoleteVersionSequenceId != null).ToList();
        }

        /// <summary>
        /// Get versioned relationships for the object
        /// </summary>
        public IEnumerable<TObject> GetRelations<TObject>(Guid? sourceKey) where TObject : IdentifiedData, ISimpleAssociation, new()
        {
            return this.Query<TObject>(o => o.SourceEntityKey == sourceKey).ToList();
        }


        /// <summary>
        /// Query the specified object
        /// </summary>
        public IEnumerable<TObject> Query<TObject>(Expression<Func<TObject, bool>> query) where TObject : IdentifiedData, new()
        {
            var persistenceService = ApplicationServiceContext.Current.GetService<IDataPersistenceService<TObject>>();
            if (persistenceService != null)
            {
                var tr = 0;
                return persistenceService.Query(query, 0, null, out tr, AuthenticationContext.Current.Principal);

            }
            return new List<TObject>();
        }

        #endregion

    }
}