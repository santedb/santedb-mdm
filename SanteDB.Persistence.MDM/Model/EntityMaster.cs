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
 * Date: 2022-5-30
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
using SanteDB.Core.Services;

namespace SanteDB.Persistence.MDM.Model
{
    /// <summary>
    /// Represents a relationship
    /// </summary>
    [XmlType(nameof(EntityRelationshipMaster), Namespace = "http://santedb.org/model")]
    public class EntityRelationshipMaster : EntityRelationship, IMdmRedirectedRelationship
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
        [SerializationMetadata, XmlIgnore, JsonProperty("$type")]
        public override string Type { get => $"EntityRelationshipMaster"; set { } }

        /// <summary>
        /// Gets the original relationship
        /// </summary>
        [SerializationMetadata, XmlElement("originalHolder"), JsonProperty("originalHolder")]
        public Guid? OriginalHolderKey { get; set; }

        /// <summary>
        /// Gets the original relationship
        /// </summary>
        [SerializationMetadata, XmlElement("originalTarget"), JsonProperty("originalTarget")]
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
    [XmlInclude(typeof(EntityRelationshipMaster)), NonCached]
    public class EntityMaster<T> : Entity, IMdmMaster<T>
        where T : Entity, new()
    {
        /// <summary>
        /// Get the type name
        /// </summary>
        [SerializationMetadata, XmlIgnore, JsonProperty("$type")]
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
            using (AuthenticationContext.EnterSystemContext())
            {
                this.m_recordOfTruth = this.LoadCollection<EntityRelationship>("Relationships").FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.MasterRecordOfTruthRelationship)?.LoadProperty(o => o.TargetEntity);
            }
        }

        /// <summary>
        /// Get the constructed master record
        /// </summary>
        public T Synthesize(IPrincipal principal)
        {
            
            var master = new T();
            master.CopyObjectData<IdentifiedData>(this.m_masterRecord, onlyNullFields: true, overwritePopulatedWithNull: false, ignoreTypeMismatch: true);

            // Is there a relationship which is the record of truth
            var pep = ApplicationServiceContext.Current.GetService<IPrivacyEnforcementService>();
            var locals = this.LocalRecords.Select(o => pep != null ? pep.Apply(o, principal) : o).OfType<T>().ToArray();

            if (locals.Length == 0) // Not a single local can be viewed
            {
                // Attempt to fetch a "replaced-by" link
                var oldRelationships = EntitySource.Current.Provider.Query<EntityRelationship>(o => (o.RelationshipTypeKey == EntityRelationshipTypeKeys.Replaces || o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship) && o.ObsoleteVersionSequenceId == null && o.TargetEntityKey == master.Key);

                var originalMasterFor = oldRelationships.FirstOrDefault(o => o.RelationshipTypeKey == MdmConstants.OriginalMasterRelationship)?.LoadProperty(o => o.SourceEntity);
                if (originalMasterFor != null)
                {
                    master.SemanticCopy(originalMasterFor);
                }
                else
                {
                    return null;
                }
            }
            else if (this.m_recordOfTruth == null) // We have to create a synthetic record
            {
                master.SemanticCopy(locals);
            }
            else // there is a ROT so use it to override the values
            {
                master.SemanticCopy((T)(object)this.m_recordOfTruth);
                master.SemanticCopyNullFields(locals);
                master.AddTag(MdmConstants.MdmRotIndicatorTag, "true");
            }

            Array.ForEach(locals, l => master.CopyAnnotations(l));
            // HACK: Copy targets for relationships and refactor them - Find a cleaner way to do this
            var relationships = new List<EntityRelationship>();
            foreach (var rel in master.LoadCollection(o => o.Relationships).Where(r => r.SourceEntityKey == master.Key)
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
                if (!relationships.Any(r => r.SemanticEquals(rel) || r.SourceEntityKey == rel.SourceEntityKey && r.TargetEntityKey == rel.TargetEntityKey && r.RelationshipTypeKey == rel.RelationshipTypeKey))
                    relationships.Add(rel);
            }
            master.Relationships = relationships;

            master.Policies = this.LocalRecords.SelectMany(o => (o as Entity).Policies).Distinct().ToList();
            master.RemoveAllTags(o => o.TagKey == MdmConstants.MdmTypeTag);
            master.AddTag(MdmConstants.MdmTypeTag, "M"); // This is a master
            master.AddTag(MdmConstants.MdmResourceTag, typeof(T).Name); // The original resource of the master
            master.AddTag(MdmConstants.MdmGeneratedTag, "true"); // This object was generated
            if (locals.Any())
            {
                master.AddTag(SanteDBConstants.AlternateKeysTag, String.Join(",", locals.Select(o => o.Key.ToString())));
            }

            master.CreationTime = this.ModifiedOn;
            master.PreviousVersionKey = this.m_masterRecord.PreviousVersionKey;
            master.StatusConceptKey = this.m_masterRecord.StatusConceptKey;
            master.Key = this.m_masterRecord.Key;
            master.VersionKey = this.m_masterRecord.VersionKey;
            master.VersionSequence = this.m_masterRecord.VersionSequence;

            // Set load state
            master.SetLoaded(o => o.Addresses);
            master.SetLoaded(o => o.Extensions);
            master.SetLoaded(o => o.Identifiers);
            master.SetLoaded(o => o.Names);
            master.SetLoaded(o => o.Notes);
            master.SetLoaded(o => o.Policies);
            master.SetLoaded(o => o.Relationships);
            master.SetLoaded(o => o.Tags);
            master.SetLoaded(o => o.Telecoms);

            switch (master)
            {
                case Person psn:
                    psn.SetLoaded(o => o.LanguageCommunication);
                    break;
                case Place plc:
                    plc.SetLoaded(o => o.Services);
                    break;
            }

            return master;
        }

        /// <summary>
        /// Modified on
        /// </summary>
        public override DateTimeOffset ModifiedOn => this.m_recordOfTruth?.ModifiedOn ?? this.LocalRecords?.OrderByDescending(o => o.ModifiedOn).OfType<BaseEntityData>().FirstOrDefault()?.ModifiedOn ?? this.m_masterRecord.ModifiedOn;

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
                    this.m_localRecords = EntitySource.Current.Provider.Query<EntityRelationship>(o => o.TargetEntityKey == this.Key && o.RelationshipTypeKey == MdmConstants.MasterRecordRelationship).Select(o => o.SourceEntityKey).ToArray()
                        .Select(o =>
                        {
                            using (DataPersistenceControlContext.Create(LoadMode.SyncLoad))
                            {
                                return EntitySource.Current.Provider.Get<T>(o);
                            }
                        }).ToList();
                }
                return this.m_localRecords;
            }
        }

        /// <summary>
        /// Get master record
        /// </summary>
        IIdentifiedData IMdmMaster.Synthesize(IPrincipal principal) => this.Synthesize(principal);

        /// <summary>
        /// Gets local records
        /// </summary>
        [XmlIgnore, JsonIgnore]
        IEnumerable<IIdentifiedData> IMdmMaster.LocalRecords => this.LocalRecords.OfType<IIdentifiedData>();
    }
}