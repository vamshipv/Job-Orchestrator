import { Component, inject, OnInit, signal } from '@angular/core';
import { Router } from '@angular/router';
import { PipelineApiService } from '../../services/pipeline-api.service';
import { PipelineDefinition } from '../../models/pipeline.model';

@Component({
  selector: 'app-pipeline-catalog',
  standalone: true,
  templateUrl: './pipeline-catalog.html',
  styleUrl: './pipeline-catalog.scss'
})
export class PipelineCatalogComponent implements OnInit {
  private api = inject(PipelineApiService);
  private router = inject(Router);

  // Angular Signals for state:
  pipelines = signal<PipelineDefinition[]>([]);
  isLoading = signal<boolean>(true);
  errorMessage = signal<string | null>(null);
  triggeringId = signal<string | null>(null);

  ngOnInit(): void {
    this.loadPipelines();
  }

  loadPipelines(): void {
    this.isLoading.set(true);
    this.errorMessage.set(null);

    this.api.getPipelines().subscribe({
      next: (data) => {
        this.pipelines.set(data);
        this.isLoading.set(false);
      },
      error: (err) => {
        console.error('Failed to load pipelines:', err);
        this.errorMessage.set('Could not connect to .NET backend. Make sure `dotnet run` is active.');
        this.isLoading.set(false);
      }
    });
  }

  onTrigger(pipeline: PipelineDefinition): void {
    this.triggeringId.set(pipeline.id);

    this.api.triggerPipeline(pipeline.id).subscribe({
      next: (res) => {
        this.triggeringId.set(null);
        // Navigate immediately to the run monitor page!
        this.router.navigate(['/runs', res.runId]);
      },
      error: (err) => {
        console.error('Trigger failed:', err);
        alert('Failed to trigger pipeline. Check backend logs.');
        this.triggeringId.set(null);
      }
    });
  }
}