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
 * Date: 2021-8-5
 */

using Newtonsoft.Json;
using SanteDB.Core;
using SanteDB.Core.Model;
using SanteDB.Core.Model.DataTypes;
using SanteDB.Core.Model.Entities;
using SanteDB.Core.Model.EntityLoader;
using SanteDB.Core.Model.Security;
using SanteDB.Core.Security.Audit;
using SanteDB.Core.Security.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Xml.Serialization;
using SanteDB.Core.Model.Roles;
using SanteDB.Core.Model.Interfaces;
using SanteDB.Core.Security;
using SanteDB.Core.Model.Constants;
using SanteDB.Core.Model.Attributes;

namespace SanteDB.Persistence.MDM.Model
{
    /// <summary>
    /// Represents a relationship
    /// </summary>
    [XmlType(Namespace = "http://santedb.org/model")]
    public class EntityRelationshipMaster : EntityRelationship
    {
        /// <summary>
        /// Default ctor for serialization
        /// </summary>
        public EntityRelationshipMaster()
        {
        }

        /// <summary>
        /// Construct an ER master
        /// </summary>
        public EntityRelationshipMaster(Entity master, Entity local, EntityRelationship relationship)
        {
            this.OriginalHolderKey = relationship.HolderKey;
            this.OriginalTargetKey = relationship.TargetEntityKey;
            this.Key = relationship.Key; // HACK: This is just for the FHIR layer to track RP
            this.RelationshipRoleKey = relationship.RelationshipRoleKey;
            this.RelationshipTypeKey = relationship.RelationshipTypeKey;
            this.ClassificationKey = relationship.ClassificationKey;
            this.SourceEntityKey = relationship.SourceEntityKey;
            this.TargetEntityKey = relationship.TargetEntityKey;
            this.Strength = relationship.Strength;

            if (this.SourceEntityKey == local.Key &&
                this.RelationshipTypeKey != MdmConstants.OriginalMasterRelationship &&
                this.RelationshipTypeKey != MdmConstants.CandidateLocalRelationship &&
                this.RelationshipTypeKey != MdmConstants.MasterRecordRelationship)
                this.SourceEntityKey = master.Key;
            else if (this.TargetEntityKey == local.Key &&
                this.RelationshipTypeKey != MdmConstants.MasterRecordOfTruthRelationship)
                this.TargetEntityKey = master.Key;

            // Does the target point at a local which has a master? If so, we want to synthesize the
            var targetMaster = this.GetTargetAs<Entity>().GetRelationships().FirstOrDefault(mr => mr.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
            if (targetMaster != null)
                this.TargetEntityKey = targetMaster.TargetEntityKey;
            this.m_annotations.Clear();
        }

        /// <summary>
        /// Get the type name
        /// </summary>
        [DataIgnore, XmlIgnore, JsonProperty("$type")]
        public override string Type { get => $"EntityRelationshipMaster"; set { } }

        /// <summary>
        /// Gets the original relationship
        /// </summary>
        [DataIgnore, XmlElement("originalHolder"), JsonProperty("originalHolder")]
        public Guid? OriginalHolderKey { get; set; }

        /// <summary>
        /// Gets the original relationship
        /// </summary>
        [DataIgnore, XmlElement("originalTarget"), JsonProperty("originalTarget")]
        public Guid? OriginalTargetKey { get; set; }
    }

    /*
     * This is an alternate in which MASTER is the sub
    /// <summary>
    /// Represents a relationship
    /// </summary>
    [XmlType(Namespace = "http://santedb.org/model")]
    public class EntityRelationshipMaster : EntityRelationship
    {
        /// <summary>
        /// Construct an ER master
        /// </summary>
        public EntityRelationshipMaster(Entity master, Entity local, EntityRelationship relationship)
        {
            this.CopyObjectData(relationship);

            this.MasterRelationship = new EntityRelationship()
            {
                RelationshipRoleKey = relationship.RelationshipRoleKey,
                RelationshipTypeKey = relationship.RelationshipTypeKey,
                ClassificationKey = relationship.ClassificationKey,
                SourceEntityKey = relationship.SourceEntityKey == local.Key ? master.Key : relationship.SourceEntityKey,
                TargetEntityKey = relationship.TargetEntityKey== local.Key ? master.Key : relationship.TargetEntityKey
            };

            // Does the target point at a local which has a master? If so, we want to synthesize the
            var targetMaster = this.MasterRelationship.GetTargetAs<Entity>().GetRelationships().FirstOrDefault(mr => mr.RelationshipTypeKey == MdmConstants.MasterRecordRelationship);
            if (targetMaster != null)
                this.MasterRelationship.TargetEntityKey = targetMaster.TargetEntityKey;
        }

        /// <summary>
        /// Get the type name
        /// </summary>
        [DataIgnore, XmlIgnore, JsonProperty("$type")]
        public override string Type { get => $"EntityRelationshipMaster"; set { } }

        /// <summary>
        /// Gets the master relationship
        /// </summary>
        [DataIgnore, XmlElement("master"), JsonProperty("master")]
        public EntityRelationship MasterRelationship { get; set; }
    }*/

    /// <summary>
    /// Represents a master record of an entity
    /// </summary>
    [XmlType(Namespace = "http://santedb.org/model")]
    [XmlInclude(typeof(EntityRelationshipMaster))]
    public class EntityMaster<T> : Entity, IMdmMaster<T>
        where T : IdentifiedData, new()
    {
        /// <summary>
        /// Get the type name
        /// </summary>
        [DataIgnore, XmlIgnore, JsonProperty("$type")]
        public override string Type { get => $"{typeof(T).Name}Master"; set { } }

        // The master record
        private Entity m_masterRecord;

        // Record of truth
        private Entity m_recordOfTruth;

        // Local records
        private List<T> m_localRecords;

        /// <summary>
        /// Create entity master
        /// </summary>
        public EntityMaster() : base()
        {
            this.ClassConceptKey = MdmConstants.MasterRecordClassification;
            if (!typeof(Entity).IsAssignableFrom(typeof(T)))
                throw new ArgumentOutOfRangeException("T must be Entity or subtype of Entity");
        }

        /// <summary>
        /// Construct an entity master record
        /// </summary>
        public EntityMaster(Entity master) : this()
        {
            this.CopyObjectData(master, false, true);
            this.m_masterRecord = master;
            this.m_recordOfTruth = this.LoadCollection<EntityRelationship>("Relationships").FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship)?.LoadProperty(o => o.TargetEntity);
        }

        /// <summary>
        /// Get the constructed master reord
        /// </summary>
        public T Synthesize(IPrincipal principal)
        {
            var master = new T();
            var entityMaster = master as Entity;
            master.CopyObjectData<IdentifiedData>(this.m_masterRecord, overwritePopulatedWithNull: false, ignoreTypeMismatch: true);

            // Is there a relationship which is the record of truth
            var pep = ApplicationServiceContext.Current.GetService<IPrivacyEnforcementService>();
            var locals = this.LocalRecords.Select(o => pep != null ? pep.Apply(o, principal) : o).OfType<T>().ToArray();

            if (locals.Length == 0) // Not a single local can be viewed
                return master;
            else if (this.m_recordOfTruth == null) // We have to create a synthetic record
            {
                master.SemanticCopy(locals);
            }
            else // there is a ROT so use it to override the values
            {
                master.SemanticCopy((T)(object)this.m_recordOfTruth);
                master.SemanticCopyNullFields(locals);
                entityMaster.Tags.Add(new EntityTag(MdmConstants.MdmRotIndicatorTag, "true"));
            }

            // HACK: Copy targets for relationships and refactor them - Find a cleaner way to do this
            var relationships = new List<EntityRelationship>();
            foreach (var rel in entityMaster.LoadCollection(o => o.Relationships).Where(r => r.SourceEntityKey == entityMaster.Key)
                .Union(
                    // Rewrite local record regular relationships
                    this.LocalRecords.OfType<Entity>().SelectMany(
                            o => o.Relationships.Where(r => r.TargetEntityKey != this.m_masterRecord.Key && r.SourceEntityKey != this.m_masterRecord.Key)
                            .Select(r => new EntityRelationshipMaster(this.m_masterRecord, o, r))
                    ))
                .Union(
                        // Select MDM relationships conveyed on the locals
                        this.LocalRecords.OfType<Entity>().SelectMany(o => o.Relationships.Where(r => r.TargetEntityKey == this.m_masterRecord.Key || r.SourceEntityKey == this.m_masterRecord.Key))
                    ))
            {
                if (!relationships.Any(r => r.SemanticEquals(rel)))
                    relationships.Add(rel);
            }
            entityMaster.Relationships = relationships;

            entityMaster.Policies = this.LocalRecords.SelectMany(o => (o as Entity).Policies).Distinct().ToList();
            entityMaster.Tags.RemoveAll(o => o.TagKey == "$mdm.type");
            entityMaster.Tags.Add(new EntityTag(MdmConstants.MdmTypeTag, "M")); // This is a master
            entityMaster.Tags.Add(new EntityTag(MdmConstants.MdmResourceTag, typeof(T).Name)); // The original resource of the master
            entityMaster.Tags.Add(new EntityTag(MdmConstants.MdmGeneratedTag, "true")); // This object was generated
            entityMaster.CreationTime = this.ModifiedOn;
            entityMaster.PreviousVersionKey = this.m_masterRecord.PreviousVersionKey;
            entityMaster.Key = this.m_masterRecord.Key;
            entityMaster.VersionKey = this.m_recordOfTruth?.VersionKey ?? this.m_masterRecord.VersionKey;
            entityMaster.VersionSequence = this.m_masterRecord.VersionSequence;
            return master;
        }

        /// <summary>
        /// Modified on
        /// </summary>
        public override DateTimeOffset ModifiedOn => this.m_recordOfTruth?.ModifiedOn ?? this.m_localRecords?.OrderByDescending(o => o.ModifiedOn).OfType<BaseEntityData>().FirstOrDefault().ModifiedOn ?? DateTime.Now;

        /// <summary>
        /// Get the version tag
        /// </summary>
        public override string Tag => this.m_recordOfTruth?.Tag ?? this.m_localRecords?.OrderByDescending(o => o.ModifiedOn).OfType<BaseEntityData>().FirstOrDefault().Tag ?? base.Tag;

        /// <summary>
        /// Get the local records of this master
        /// </summary>
        [XmlIgnore, JsonIgnore]
        public IEnumerable<T> LocalRecords
        {
            get
            {
                if (this.m_localRecords == null)
                {
                    this.m_localRecords = EntitySource.Current.Provider.Query<EntityRelationship>(o => o.TargetEntityKey == this.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Select(o => o.LoadProperty<T>("SourceEntity")).ToList();
                    this.m_localRecords.OfType<Entity>().ToList().ForEach(o =>
                    {
                        o.LoadCollection(p => p.Relationships, true);
                        o.LoadCollection<EntityAddress>(nameof(Entity.Addresses));
                        o.LoadCollection<EntityTag>(nameof(Entity.Tag));
                        o.LoadCollection<EntityTelecomAddress>(nameof(Entity.Telecoms));
                        o.LoadCollection<EntityIdentifier>(nameof(Entity.Identifiers));
                        o.LoadCollection<EntityName>(nameof(Entity.Names));
                        o.LoadCollection<EntityNote>(nameof(Entity.Notes));
                        o.LoadCollection<SecurityPolicyInstance>(nameof(Entity.Policies), true);
                        o.LoadCollection<EntityExtension>(nameof(Entity.Extensions));

                        if (o is Place place)
                            place.LoadCollection<PlaceService>(nameof(Place.Services));
                        if (o is Person person)
                            person.LoadCollection<PersonLanguageCommunication>(nameof(Person.LanguageCommunication));
                    });
                }
                return this.m_localRecords;
            }
        }

        /// <summary>
        /// Get master record
        /// </summary>
        IIdentifiedEntity IMdmMaster.GetMaster(IPrincipal principal) => this.Synthesize(principal);

        /// <summary>
        /// Gets local records
        /// </summary>
        [XmlIgnore, JsonIgnore]
        IEnumerable<IIdentifiedEntity> IMdmMaster.LocalRecords => this.LocalRecords.OfType<IIdentifiedEntity>();
    }
}