export interface Column {
  key: string;
  name: string;
  data_type: string | null;
  is_nullable: boolean | null;
}

export interface Table {
  key: string;
  name: string;
  type: string | null;
  source_file: string | null;
}

export interface Transformation {
  key: string;
  type: string;
  expression: string;
}

export interface TableDetail extends Table {
  columns: Column[];
  transformations: Transformation[];
}

export interface LineageNode {
  key: string;
  name: string;
  data_type: string | null;
  is_nullable: boolean | null;
  // Optional relationship attribute returned by the API for each hop
  // (e.g. transformation used to derive the center column).
  transformation?: string | null;
}

// Parsed hierarchy for the schema explorer tree
export interface SchemaTree {
  [database: string]: {
    [schema: string]: Table[];
  };
}
