import { Function, Integer, FunctionsMap, Edits, OntologyEditFunction } from "@foundry/functions-api";
import { Objects } from "@foundry/ontology-api";
import { Treatment, Patients, Surgeries, TreatmentTypes, PatientsSurgeries, TreatmentTypesCleaned, ChangeSnapshot, ChangeAlertTable } from "@foundry/ontology-api";
import { getDownstream, getAllDownstream, dependencyMap } from "./dependency_map";

interface ObjectSnapshot {
  objectTypeName: string;
  schema: string[];
  rowCount: Integer;
  timestamp: string;
  hash: string;
}

interface ChangeDetectionResult {
  objectTypeName: string;
  changeType: string;
  currentSnapshot: ObjectSnapshot;
  previousSnapshot?: ObjectSnapshot;
  details: string;
  schemaChanges?: {
    added: string[];
    removed: string[];
  };
}

export class Change_detector_new {
    
    @Edits(ChangeSnapshot, ChangeAlertTable)
    @OntologyEditFunction()
    @Function()
    public async monitorDatasetChanges(): Promise<void> {
        const currentTime = new Date().toISOString();

        const monitoringConfig = [
            {
                name: "TreatmentTypes",
                objectSet: Objects.search().treatment(),
                rid: "ri.foundry.main.dataset.4ed17202-986c-4ab1-821c-829cefc2ba4d",
                expectedSchema: ['treatment_name', 'treatment_type_id', 'implant_type', 'surgical_method']
            },
            {
                name: "Patients",
                objectSet: Objects.search().patients(),
                rid: "ri.foundry.main.dataset.b4e1e219-d5fd-4dea-bb8e-5058dd3b8f93",
                expectedSchema: ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name']
            },
            {
                name: "Surgeries",
                objectSet: Objects.search().surgeries(),
                rid: "ri.foundry.main.dataset.b477a9de-a00f-487d-844c-a52004e192f1",
                expectedSchema: ['date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'patient_id']
            },
            {
                name: "Treatments",
                objectSet: Objects.search().treatmentTypes(),
                rid: "ri.foundry.main.dataset.de9f0a83-ae84-4d1e-9bce-07d4ecadcfb1",
                expectedSchema: ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'surgery_name']
            },
            {
                name: "PatientsSurgeries",
                objectSet: Objects.search().patientsSurgeries(),
                rid: "ri.foundry.main.dataset.9946e288-6f73-4aed-84d2-aa4c9df68145",
                expectedSchema: ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name', 'date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'surgery_date', 'primary_key']
            },
            {
                name: "TreatmentTypesCleaned",
                objectSet: Objects.search().treatmentTypesCleaned(),
                rid: "ri.foundry.main.dataset.8d1ad951-c2f8-4a73-b20c-4c720cdcb060", 
                expectedSchema: ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'date','surgery_name']
            }
        ];

        for (const config of monitoringConfig) {
            try {
                // Use allAsync to avoid pagination errors
                const allObjects = await config.objectSet.allAsync(); 
                const currentRowCount = allObjects.length;
                
                // Use the first object to infer schema
                const sampleObjects = allObjects.slice(0, 1);
                
                const currentSchema = this.extractSchemaFromObjects(sampleObjects, undefined, config.expectedSchema, config.name);

                const currentSnapshot: ObjectSnapshot = {
                    objectTypeName: config.name,
                    schema: currentSchema,
                    rowCount: currentRowCount,
                    timestamp: currentTime,
                    hash: this.generateSnapshotHash(currentSchema, currentRowCount)
                };

                const previousSnapshot = await this.getPreviousSnapshot(config.name);
                const changeResult = this.compareSnapshots(currentSnapshot, previousSnapshot);

                // Save Snapshot
                await this.saveSnapshotAction(
                    currentSnapshot.objectTypeName,
                    currentSnapshot.schema,
                    currentSnapshot.rowCount,
                    currentSnapshot.timestamp,
                    currentSnapshot.hash
                );

                console.log(`Change detection result for ${config.name}:`, changeResult);
                
                if (changeResult.changeType !== 'NO_CHANGE') {
                    const directImpacted = getDownstream(config.name);
                    
                    console.log(`Creating Alert for ${config.name}. Impacted: ${directImpacted}`);

                    // Create Alert
                    await this.createImpactAlert(
                        currentSnapshot,
                        directImpacted,
                        changeResult.changeType
                    );
                }

            } catch (error) {
                console.error(`Error processing ${config.name}:`, error);
            }
        }
    }

    private async createImpactAlert(
        current: ObjectSnapshot, 
        downstream: string[], 
        changeType: string
    ): Promise<void> {
        const alertId = `ALERT-${Date.now()}-${Math.random().toString(36).substr(2, 5)}`;
        
        const newAlert = Objects.create().changeAlertTable(alertId);
        
        newAlert.triggerDataset = current.objectTypeName;
        newAlert.changeType = changeType;
        newAlert.impactScore = downstream.length;
        newAlert.downstreamDatasets = JSON.stringify(downstream);
        
        // --- FIXED: Use 'new Date()' to satisfy the Ontology ---
        // 'as any' is still needed to bypass strict SDK types, but passing a real Date object fixes the runtime error.
        newAlert.timestamp = new Date(current.timestamp) as any; 

        console.log(`Generated Impact Alert ${alertId} for ${current.objectTypeName}`);
    }

    private async saveSnapshotAction(
        objectTypeName: string,
        schema: string[],
        rowCount: Integer,
        timestamp: string,
        hash: string
        ): Promise<void> {
        try {
            const snapshotId = `${objectTypeName}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            
            const newSnapshot = Objects.create().changeSnapshot(snapshotId);
            
            newSnapshot.objectTypeName = objectTypeName;
            newSnapshot.schema = [...schema];
            newSnapshot.rowCount = rowCount;
            newSnapshot.timestamp = timestamp;
            newSnapshot.hash = hash;

            console.log(`Staged snapshot for ${objectTypeName}`);
            
        } catch (error) {
            console.error(`Error saving snapshot for ${objectTypeName}:`, error);
            throw error;
        }
    }

    // --- HELPER METHODS ---

    private extractSchemaFromObjects(objects: readonly any[], sampleObject?: any, expectedSchema?: string[], objectTypeName?: string): string[] {
        if (expectedSchema && expectedSchema.length > 0) return expectedSchema;
        if (objects.length > 0) return this.extractSchemaFromSingleObject(objects[0], objectTypeName);
        return [];
    }

    private extractSchemaFromSingleObject(obj: any, objectTypeName?: string): string[] {
        if (!obj) return [];
        return Object.keys(obj).filter(key => 
            !key.startsWith('$') && 
            !key.startsWith('_') && 
            key !== 'rid' && 
            key !== 'primaryKey' && 
            key !== 'osp' && 
            key !== 'typeId'
        );
    }

    private generateSnapshotHash(schema: string[], rowCount: number): string {
        return JSON.stringify({ schema: schema.sort(), rowCount });
    }

    private async getPreviousSnapshot(objectTypeName: string): Promise<ObjectSnapshot | undefined> {
        try {
            const snapshots = await Objects.search().changeSnapshot()
                .filter(snapshot => snapshot.objectTypeName.exactMatch(objectTypeName))
                .orderBy(snapshot => snapshot.timestamp.desc())
                .takeAsync(1);

            if (snapshots.length > 0) {
                return {
                    objectTypeName: snapshots[0].objectTypeName || '',
                    schema: snapshots[0].schema ? [...snapshots[0].schema] : [],
                    rowCount: snapshots[0].rowCount || 0,
                    timestamp: snapshots[0].timestamp || '',
                    hash: snapshots[0].hash || ''
                };
            }
            return undefined;
        } catch (error) {
            console.error(`Error getting previous snapshot:`, error);
            return undefined;
        }
    }

    private compareSnapshots(current: ObjectSnapshot, previous?: ObjectSnapshot): ChangeDetectionResult {
        if (!previous) {
            return {
                objectTypeName: current.objectTypeName,
                changeType: 'NEW_OBJECT_TYPE',
                currentSnapshot: current,
                details: 'First time monitoring'
            };
        }

        const schemaChanges = this.detectSchemaChanges(current.schema, previous.schema);
        if (schemaChanges.added.length > 0 || schemaChanges.removed.length > 0) {
            return {
                objectTypeName: current.objectTypeName,
                changeType: 'SCHEMA_CHANGED',
                currentSnapshot: current,
                previousSnapshot: previous,
                schemaChanges: schemaChanges,
                details: `Schema changed`
            };
        }

        if (current.rowCount !== previous.rowCount) {
            return {
                objectTypeName: current.objectTypeName,
                changeType: 'ROW_COUNT_CHANGED',
                currentSnapshot: current,
                previousSnapshot: previous,
                details: `Row count changed`
            };
        }

        return {
            objectTypeName: current.objectTypeName,
            changeType: 'NO_CHANGE',
            currentSnapshot: current,
            previousSnapshot: previous,
            details: 'No changes detected'
        };
    }

    private detectSchemaChanges(currentSchema: string[], previousSchema: string[]): { added: string[], removed: string[] } {
        const currentSet = new Set(currentSchema);
        const previousSet = new Set(previousSchema);
        return { added: currentSchema.filter(p => !previousSet.has(p)), removed: previousSchema.filter(p => !currentSet.has(p)) };
    }
}
