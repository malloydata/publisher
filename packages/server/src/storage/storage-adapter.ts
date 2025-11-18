export interface StorageAdapter {
  init(): Promise<void>;

  getState(key: string): Promise<any | null>;
  setState(key: string, value: any): Promise<void>;
  deleteState(key: string): Promise<void>;
}