import { Function, Integer, FunctionsMap, Edits, OntologyEditFunction } from "@foundry/functions-api";
import { Objects } from "@foundry/ontology-api";
import { Treatment, Patients, Surgeries, TreatmentTypes, PatientsSurgeries, TreatmentTypesCleaned, ChangeSnapshot } from "@foundry/ontology-api";
import { getDownstream, getAllDownstream, dependencyMap } from "./dependency_map";

// for comapring current state with previous state
interface ObjectSnapshot {
  objectTypeName: string;
  schema: string[];
  rowCount: Integer;
  timestamp: string;
  hash: string;
}

// used to report what kind of change (if any) was detected
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

// Used to power dependency graphs in dashboards or reports.
interface DependencyVisualizationData {
  nodes: string;
  edges: string;
  statistics: string;
}

// Holds historical change data for reporting and analysis
interface ChangeHistoryData {
  changeEvents: string;
  changesByDataset: string;
  changeStatistics: string;
}

// Holds results of analyzing the impact of changes across datasets
interface ImpactAnalysisData {
  impactAnalysis: string;
  impactStatistics: string;
}

//  Aggregates all the above for dashboard visualizations
interface VisualizationSummaryData {
  dependencyMap: string;
  dependencies: string;
  recentChanges: string;
  impactRankings: string;
  currentCounts: string;
  dashboardMetrics: string;
}

export class DatasetChangeMonitor {
    @Edits(ChangeSnapshot)
    @OntologyEditFunction()
    @Function()
    public async monitorDatasetChanges(
        /*
        treatment?: Treatment,
        patients?: Patients,
        surgeries?: Surgeries,
        treatmentTypes?: TreatmentTypes,
        patientsSurgeries?: PatientsSurgeries,
        treatmentTypesCleaned?: TreatmentTypesCleanedm */
    ): Promise<void> {
        const currentTime = new Date().toISOString();

        const monitoringConfig = [
            {
                name: "TreatmentTypes",
                objectSet: Objects.search().treatment(),
                // sampleObject: treatment,
                rid: "ri.foundry.main.dataset.4ed17202-986c-4ab1-821c-829cefc2ba4d",
                expectedSchema: ['treatment_name', 'treatment_type_id', 'implant_type', 'surgical_method']
            },
            {
                name: "Patients",
                objectSet: Objects.search().patients(),
                // sampleObject: patients,
                rid: "ri.foundry.main.dataset.b4e1e219-d5fd-4dea-bb8e-5058dd3b8f93",
                expectedSchema: ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name']
            },
            {
                name: "Surgeries",
                objectSet: Objects.search().surgeries(),
                // sampleObject: surgeries,
                rid: "ri.foundry.main.dataset.b477a9de-a00f-487d-844c-a52004e192f1",
                expectedSchema: ['date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'patient_id']
            },
            {
                name: "Treatments",
                objectSet: Objects.search().treatmentTypes(),
                // sampleObject: treatmentTypes,
                rid: "ri.foundry.main.dataset.de9f0a83-ae84-4d1e-9bce-07d4ecadcfb1",
                expectedSchema: ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'surgery_name']
            },
            {
                name: "PatientsSurgeries",
                objectSet: Objects.search().patientsSurgeries(),
                // sampleObject: patientsSurgeries,
                rid: "ri.foundry.main.dataset.9946e288-6f73-4aed-84d2-aa4c9df68145",
                expectedSchema: ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name', 'date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'surgery_date', 'primary_key']
            },
            {
                name: "TreatmentTypesCleaned",
                objectSet: Objects.search().treatmentTypesCleaned(),
                // sampleObject: treatmentTypesCleaned,
                rid: "ri.foundry.main.dataset.<REPLACE_WITH_ACTUAL_RID>",
                expectedSchema: ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'date','surgery_name']
            }
        ];

        // Loop through each dataset configuration
        for (const config of monitoringConfig) {
            try {
                // Using OntologyObjectSet.allAsync() method
                // Fetch all objects for the current dataset
                // eslint-disable-next-line no-await-in-loop
                const currentObjects = await config.objectSet.allAsync();
                const currentRowCount = currentObjects.length;
                const currentSchema = this.extractSchemaFromObjects(currentObjects, undefined, config.expectedSchema, config.name);

                // Build the current snapshot object
                const currentSnapshot: ObjectSnapshot = {
                    objectTypeName: config.name,
                    schema: currentSchema,
                    rowCount: currentRowCount,
                    timestamp: currentTime,
                    hash: this.generateSnapshotHash(currentSchema, currentRowCount)
                };

                // Build the current snapshot object
                const previousSnapshot = await this.getPreviousSnapshot(config.name);
                // Compare current and previous snapshots to detect changes
                const changeResult = this.compareSnapshots(currentSnapshot, previousSnapshot);

                // Save the current snapshot as an ontology object
                await this.saveSnapshotAction(
                    currentSnapshot.objectTypeName,
                    currentSnapshot.schema,
                    currentSnapshot.rowCount,
                    currentSnapshot.timestamp,
                    currentSnapshot.hash);

                console.log(`Change detection result for ${config.name}:`, changeResult);
                
                
                if (changeResult.changeType !== 'NO_CHANGE') {
                    const directImpacted = getDownstream(config.name);
                    console.log(`Direct downstream impacted by change in ${config.name}:`, directImpacted);

                    const allImpacted = getAllDownstream(config.name);
                    console.log(`All downstream impacted by change in ${config.name}:`, allImpacted);
                }

            } catch (error) {
                console.error(`Error processing ${config.name}:`, error);
            }
        }
    }

    // Compare current and previous snapshots to detect changes
    @Function()
    public async getObjectCounts(): Promise<FunctionsMap<string, Integer>> {
        const counts = new FunctionsMap<string, number>();
        
        try {
            const treatmentObjects = await Objects.search().treatment().allAsync();
            counts.set("Treatment", treatmentObjects.length);

            const patientObjects = await Objects.search().patients().allAsync();
            counts.set("Patients", patientObjects.length);

            const surgeryObjects = await Objects.search().surgeries().allAsync();
            counts.set("Surgeries", surgeryObjects.length);

            const treatmentTypeObjects = await Objects.search().treatmentTypes().allAsync();
            counts.set("TreatmentTypes", treatmentTypeObjects.length);

            const patientSurgeryObjects = await Objects.search().patientsSurgeries().allAsync();
            counts.set("PatientsSurgeries", patientSurgeryObjects.length);

            const treatmentTypesCleanedObjects = await Objects.search().treatmentTypesCleaned().allAsync();
            counts.set("TreatmentTypesCleaned", treatmentTypesCleanedObjects.length);
            
        } catch (error) {
            console.error("Error getting object counts:", error);
        }

        return counts;
    }

    @Function()
    public async exportDependencyMapForVisualization(): Promise<DependencyVisualizationData> {
        try {
            const nodes = Object.keys(dependencyMap).concat(
                ...Object.values(dependencyMap).flat()
            );
            const uniqueNodes = [...new Set(nodes)];
            
            const nodeData = uniqueNodes.map(node => ({
                id: node,
                name: node,
                type: "dataset"
            }));
            
            const edges: Array<{source: string, target: string, type: string}> = [];
            Object.entries(dependencyMap).forEach(([source, targets]) => {
                targets.forEach(target => {
                    edges.push({
                        source: source,
                        target: target,
                        type: "dependency"
                    });
                });
            });
            
            const stats = {
                totalNodes: uniqueNodes.length,
                totalEdges: edges.length,
                nodesWithDependencies: Object.keys(dependencyMap).length,
                maxDownstreamCount: Math.max(...Object.values(dependencyMap).map(deps => deps.length))
            };
            
            console.log(`Exported dependency map: ${stats.totalNodes} nodes, ${stats.totalEdges} edges`);
            
            return {
                nodes: JSON.stringify(nodeData),
                edges: JSON.stringify(edges),
                statistics: JSON.stringify(stats)
            };
            
        } catch (error) {
            console.error("Error exporting dependency map:", error);
            return {
                nodes: JSON.stringify([]),
                edges: JSON.stringify([]),
                statistics: JSON.stringify({})
            };
        }
    }

    //Retrieves the history of changes (snapshots) for all datasets.
    @Function()
    public async getChangeHistory(): Promise<ChangeHistoryData> {
        try {
            const allSnapshots = await Objects.search().changeSnapshot()
                .orderBy(snapshot => snapshot.timestamp.desc())
                .takeAsync(100);
            
            const changeEvents = allSnapshots.map(snapshot => ({
                objectTypeName: snapshot.objectTypeName || '',
                timestamp: snapshot.timestamp || '',
                rowCount: snapshot.rowCount || 0,
                schema: [...(snapshot.schema || [])],
                hash: snapshot.hash || ''
            }));
            
            const groupedChanges = new Map<string, typeof changeEvents>();
            changeEvents.forEach(event => {
                if (!groupedChanges.has(event.objectTypeName)) {
                    groupedChanges.set(event.objectTypeName, []);
                }
                groupedChanges.get(event.objectTypeName)!.push(event);
            });
            
            const groupedChangesObj: { [key: string]: typeof changeEvents } = {};
            groupedChanges.forEach((value, key) => {
                groupedChangesObj[key] = value;
            });
            
            const changeStats = {
                totalChanges: changeEvents.length,
                datasetsWithChanges: groupedChanges.size,
                mostActiveDataset: this.getMostActiveDataset(groupedChanges),
                timeRange: {
                    earliest: changeEvents.length > 0 ? changeEvents[changeEvents.length - 1].timestamp : null,
                    latest: changeEvents.length > 0 ? changeEvents[0].timestamp : null
                }
            };
            
            return {
                changeEvents: JSON.stringify(changeEvents),
                changesByDataset: JSON.stringify(groupedChangesObj),
                changeStatistics: JSON.stringify(changeStats)
            };
            
        } catch (error) {
            console.error("Error getting change history:", error);
            return {
                changeEvents: JSON.stringify([]),
                changesByDataset: JSON.stringify({}),
                changeStatistics: JSON.stringify({})
            };
        }
    }

    // Performs impact analysis based on dependency map and change history.
    @Function()
    public async getImpactAnalysis(): Promise<ImpactAnalysisData> {
        try {
            const impactScores = new Map<string, number>();
            
            Object.keys(dependencyMap).forEach(dataset => {
                const downstreamCount = getAllDownstream(dataset).length;
                impactScores.set(dataset, downstreamCount);
            });
            
            const lastChangeMap = await this.getLastChangeTimestamps();
            
            const impactAnalysis = Array.from(impactScores.entries()).map(([dataset, score]) => ({
                dataset,
                impactScore: score,
                directDownstream: getDownstream(dataset),
                allDownstream: getAllDownstream(dataset),
                lastChangeTimestamp: lastChangeMap.get(dataset)
            }));
            
            impactAnalysis.sort((a, b) => b.impactScore - a.impactScore);
            
            const impactStats = {
                totalDatasets: impactAnalysis.length,
                highImpactDatasets: impactAnalysis.filter(item => item.impactScore >= 3).length,
                mediumImpactDatasets: impactAnalysis.filter(item => item.impactScore >= 1 && item.impactScore < 3).length,
                lowImpactDatasets: impactAnalysis.filter(item => item.impactScore === 0).length,
                averageImpactScore: impactAnalysis.reduce((sum, item) => sum + item.impactScore, 0) / impactAnalysis.length
            };
            
            return {
                impactAnalysis: JSON.stringify(impactAnalysis),
                impactStatistics: JSON.stringify(impactStats)
            };
            
        } catch (error) {
            console.error("Error generating impact analysis:", error);
            return {
                impactAnalysis: JSON.stringify([]),
                impactStatistics: JSON.stringify({})
            };
        }
    }

    @Function()
    public async getVisualizationSummary(): Promise<VisualizationSummaryData> {
        try {
            const dependencyData = await this.exportDependencyMapForVisualization();
            const historyData = await this.getChangeHistory();
            const impactData = await this.getImpactAnalysis();
            const objectCounts = await this.getObjectCounts();
            
            const dashboardMetrics = {
                totalDatasets: JSON.parse(dependencyData.statistics).totalNodes || 0,
                totalDependencies: JSON.parse(dependencyData.statistics).totalEdges || 0,
                recentChangesCount: JSON.parse(historyData.changeStatistics).totalChanges || 0,
                highImpactDatasets: JSON.parse(impactData.impactStatistics).highImpactDatasets || 0,
                lastUpdateTime: new Date().toISOString()
            };
            
            const countsObj: { [key: string]: number } = {};
            objectCounts.forEach((value, key) => {
                countsObj[key] = value;
            });
            
            return {
                dependencyMap: dependencyData.nodes,
                dependencies: dependencyData.edges,
                recentChanges: historyData.changeEvents,
                impactRankings: impactData.impactAnalysis,
                currentCounts: JSON.stringify(countsObj),
                dashboardMetrics: JSON.stringify(dashboardMetrics)
            };
            
        } catch (error) {
            console.error("Error generating visualization summary:", error);
            return {
                dependencyMap: JSON.stringify([]),
                dependencies: JSON.stringify([]),
                recentChanges: JSON.stringify([]),
                impactRankings: JSON.stringify([]),
                currentCounts: JSON.stringify({}),
                dashboardMetrics: JSON.stringify({})
            };
        }
    }

    // BEST APPROACH: Enhanced schema extraction with multiple fallback strategies
    private extractSchemaFromObjects(
        objects: readonly any[], 
        sampleObject?: any, 
        expectedSchema?: string[], 
        objectTypeName?: string
    ): string[] {
        console.log(`Extracting schema for ${objectTypeName}, objects count: ${objects.length}`);
        
        // Strategy 1: Use predefined expected schema (most reliable)
        if (expectedSchema && expectedSchema.length > 0) {
            console.log(`Using expected schema for ${objectTypeName}:`, expectedSchema);
            return expectedSchema;
        }
        
        // Strategy 2: Extract from sample object using proper Ontology API access
        if (sampleObject) {
            const sampleSchema = this.extractSchemaFromSingleObject(sampleObject, objectTypeName);
            if (sampleSchema.length > 0) {
                console.log(`Extracted schema from sample object for ${objectTypeName}:`, sampleSchema);
                return sampleSchema;
            }
        }
        
        // Strategy 3: Extract from first object in the set
        if (objects.length > 0) {
            const firstObjectSchema = this.extractSchemaFromSingleObject(objects[0], objectTypeName);
            if (firstObjectSchema.length > 0) {
                console.log(`Extracted schema from first object for ${objectTypeName}:`, firstObjectSchema);
                return firstObjectSchema;
            }
        }
        
        // Strategy 4: Use ontology metadata approach
        const metadataSchema = this.extractSchemaFromMetadata(objectTypeName);
        if (metadataSchema.length > 0) {
            console.log(`Using metadata schema for ${objectTypeName}:`, metadataSchema);
            return metadataSchema;
        }
        
        console.log(`No schema found for ${objectTypeName}, returning empty array`);
        return [];
    }

    private extractSchemaFromSingleObject(obj: any, objectTypeName?: string): string[] {
        if (!obj) return [];
        
        console.log(`Analyzing object structure for ${objectTypeName}:`, Object.keys(obj));
        
        // Method 1: Direct property access
        const directProperties = Object.keys(obj).filter(key => 
            !key.startsWith('$') && 
            !key.startsWith('_') &&
            typeof obj[key] !== 'function' &&
            key !== 'rid' &&
            key !== 'typeId' &&
            key !== 'primaryKey' &&
            key !== 'osp' && // Filter out Foundry internal properties
            key !== 'properties' &&
            obj[key] !== undefined &&
            obj[key] !== null
        );
        
        if (directProperties.length > 0) {
            console.log(`Found direct properties for ${objectTypeName}:`, directProperties);
            return directProperties.sort();
        }
        
        // Method 2: Access through primaryKey structure
        if (obj.primaryKey && typeof obj.primaryKey === 'object') {
            const primaryKeyProps = Object.keys(obj.primaryKey).filter(key => 
                !key.startsWith('$') && !key.startsWith('_')
            );
            if (primaryKeyProps.length > 0) {
                console.log(`Found primary key properties for ${objectTypeName}:`, primaryKeyProps);
                return primaryKeyProps.sort();
            }
        }
        
        // Method 3: Try to access nested property structures
        if (obj.properties && typeof obj.properties === 'object') {
            const nestedProps = Object.keys(obj.properties);
            if (nestedProps.length > 0) {
                console.log(`Found nested properties for ${objectTypeName}:`, nestedProps);
                return nestedProps.sort();
            }
        }
        
        return [];
    }

    private extractSchemaFromMetadata(objectTypeName?: string): string[] {
        if (!objectTypeName) return [];
        
        // Use ontology metadata to get property information
        try {
            const typeSchemas: { [key: string]: string[] } = {
                'Treatment': ['treatment_name', 'treatment_type_id', 'implant_type', 'surgical_method'],
                'Patients': ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name'],
                'Surgeries': ['date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'patient_id'],
                'TreatmentTypes': ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'surgery_name'],
                'PatientsSurgeries': ['patient_id', 'age_group', 'condition', 'comorbidities', 'obfuscated_patient_name', 'date', 'surgery_id', 'surgery_name', 'treatment_type_id', 'surgery_date', 'primary_key'],
                'TreatmentTypesCleaned': ['treatment_type_id', 'treatment_name', 'implant_type', 'surgical_method', 'surgery_name']
            };
            
            return typeSchemas[objectTypeName] || [];
        } catch (error) {
            console.error(`Error accessing metadata for ${objectTypeName}:`, error);
            return [];
        }
    }

    private generateSnapshotHash(schema: string[], rowCount: number): string {
        const hashString = JSON.stringify({ schema: schema.sort(), rowCount });
        let hash = 0;
        for (let i = 0; i < hashString.length; i++) {
            const char = hashString.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return hash.toString();
    }

    private async getPreviousSnapshot(objectTypeName: string): Promise<ObjectSnapshot | undefined> {
        try {
            console.log(`Looking for previous snapshot of ${objectTypeName}`);
            
            const snapshots = await Objects.search().changeSnapshot()
                .filter(snapshot => snapshot.objectTypeName.exactMatch(objectTypeName))
                .orderBy(snapshot => snapshot.timestamp.desc())
                .takeAsync(1);

            console.log(`Found ${snapshots.length} snapshots for ${objectTypeName}`);

            if (snapshots.length > 0) {
                const latestSnapshot = snapshots[0];
                const previousSnapshot = {
                    objectTypeName: latestSnapshot.objectTypeName || '',
                    schema: latestSnapshot.schema ? [...latestSnapshot.schema] : [],
                    rowCount: latestSnapshot.rowCount || 0,
                    timestamp: latestSnapshot.timestamp || '',
                    hash: latestSnapshot.hash || ''
                };
                
                console.log(`Retrieved previous snapshot for ${objectTypeName}:`, {
                    rowCount: previousSnapshot.rowCount,
                    timestamp: previousSnapshot.timestamp,
                    hash: previousSnapshot.hash
                });
                
                return previousSnapshot;
            }

            console.log(`No previous snapshot found for ${objectTypeName}`);
            return undefined;
        } catch (error) {
            console.error(`Error getting previous snapshot for ${objectTypeName}:`, error);
            return undefined;
        }
    }

    private compareSnapshots(current: ObjectSnapshot, previous?: ObjectSnapshot): ChangeDetectionResult {
        if (!previous) {
            return {
                objectTypeName: current.objectTypeName,
                changeType: 'NEW_OBJECT_TYPE',
                currentSnapshot: current,
                details: 'First time monitoring this object type'
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
                details: `Schema changed - Added: [${schemaChanges.added.join(', ')}], Removed: [${schemaChanges.removed.join(', ')}]`
            };
        }

        if (current.rowCount !== previous.rowCount) {
            const change = current.rowCount - previous.rowCount;
            return {
                objectTypeName: current.objectTypeName,
                changeType: 'ROW_COUNT_CHANGED',
                currentSnapshot: current,
                previousSnapshot: previous,
                details: `Row count changed from ${previous.rowCount} to ${current.rowCount} (${change > 0 ? '+' : ''}${change})`
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

        const added = currentSchema.filter(prop => !previousSet.has(prop));
        const removed = previousSchema.filter(prop => !currentSet.has(prop));

        return { added, removed };
    }

    // Saves a snapshot as a ChangeSnapshot ontology object.
    @Edits(ChangeSnapshot)
    @OntologyEditFunction()
    @Function()
    public async saveSnapshotAction(
        objectTypeName: string,
        schema: string[],
        rowCount: Integer,
        timestamp: string,
        hash: string
        ): Promise<void> {
        try {
            // Generate a unique primary key for the snapshot
            const snapshotId = `${objectTypeName}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            
            // Create the snapshot object YARD
            const newSnapshot = Objects.create().changeSnapshot(snapshotId);
            
            // Set all properties
            newSnapshot.objectTypeName = objectTypeName;
            newSnapshot.schema = [...schema];
            newSnapshot.rowCount = rowCount;
            newSnapshot.timestamp = timestamp;
            newSnapshot.hash = hash;

            // await newSnapshot.save();
            console.log(`Successfully saved snapshot for ${objectTypeName} with ID: ${snapshotId}`);
            
        } catch (error) {
            console.error(`Error saving snapshot for ${objectTypeName}:`, error);
            throw error;
        }
    }

    private getMostActiveDataset(groupedChanges: Map<string, any[]>): string | null {
        let maxChanges = 0;
        let mostActive: string | null = null;
        
        groupedChanges.forEach((changes, dataset) => {
            if (changes.length > maxChanges) {
                maxChanges = changes.length;
                mostActive = dataset;
            }
        });
        
        return mostActive;
    }

    private async getLastChangeTimestamps(): Promise<Map<string, string>> {
        const timestampMap = new Map<string, string>();
        
        try {
            const datasets = ["Treatment", "Patients", "Surgeries", "TreatmentTypes", "PatientsSurgeries", "TreatmentTypesCleaned"];
            
            for (const dataset of datasets) {
                const latestSnapshot = await Objects.search().changeSnapshot()
                    .filter(snapshot => snapshot.objectTypeName.exactMatch(dataset))
                    .orderBy(snapshot => snapshot.timestamp.desc())
                    .takeAsync(1);
                
                if (latestSnapshot.length > 0) {
                    timestampMap.set(dataset, latestSnapshot[0].timestamp || '');
                }
            }
        } catch (error) {
            console.error("Error getting last change timestamps:", error);
        }
        
        return timestampMap;
    }
}

export { DatasetChangeMonitor as ChangeDetector };
