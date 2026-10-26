import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { PipelineDefinition, PipelineRun } from '../models/pipeline.model';

@Injectable({
  providedIn: 'root'
})
export class PipelineApiService {
  // Modern dependency injection via inject() instead of constructor!
  private http = inject(HttpClient);

  // Set this to your .NET backend port from Day 1/2 (e.g. http://localhost:5000 or https://localhost:7xxx)
  private readonly baseUrl = 'http://localhost:5203/api';

  // 1. Fetch all pipeline definitions
  getPipelines(): Observable<PipelineDefinition[]> {
    return this.http.get<PipelineDefinition[]>(`${this.baseUrl}/pipelines`);
  }

  // 2. Fetch a single pipeline by ID
  getPipelineById(id: string): Observable<PipelineDefinition> {
    return this.http.get<PipelineDefinition>(`${this.baseUrl}/pipelines/${id}`);
  }

  // 3. Trigger a pipeline run (Returns 202 with runId)
  triggerPipeline(id: string): Observable<{ runId: string; status: string; message: string }> {
    return this.http.post<{ runId: string; status: string; message: string }>(
      `${this.baseUrl}/pipelines/${id}/trigger`,
      {}
    );
  }

  // 4. Get run status by runId
  getRunStatus(runId: string): Observable<PipelineRun> {
    return this.http.get<PipelineRun>(`${this.baseUrl}/runs/${runId}`);
  }
}