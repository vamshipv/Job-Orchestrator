import { Component, inject, OnInit, signal, computed, OnDestroy } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { DatePipe } from '@angular/common';
import { PipelineApiService } from '../../services/pipeline-api.service';
import { PipelineRun, PipelineStepDefinition } from '../../models/pipeline.model';
import { ReactDagCanvasComponent } from '../../canvas/react-dag-canvas';

@Component({
  selector: 'app-run-detail',
  standalone: true,
  imports: [RouterLink, DatePipe, ReactDagCanvasComponent],
  templateUrl: './run-detail.html',
  styleUrl: './run-detail.scss'
})
export class RunDetailComponent implements OnInit, OnDestroy {
  private route = inject(ActivatedRoute);
  private api = inject(PipelineApiService);

  runId = signal<string>('');
  run = signal<PipelineRun | null>(null);
  pipelineSteps = signal<PipelineStepDefinition[]>([]);
  isLoading = signal<boolean>(true);
  errorMessage = signal<string | null>(null);

  private pollInterval: any = null;

  status = computed(() => this.run()?.status ?? 'Queued');
  steps = computed(() => this.run()?.stepStatuses ?? []);
  totalSteps = computed(() => this.steps().length);
  completedCount = computed(() => 
    this.steps().filter(s => s.state === 'Succeeded').length
  );
  progressPercent = computed(() => {
    const total = this.totalSteps();
    return total > 0 ? Math.round((this.completedCount() / total) * 100) : 0;
  });
  isCompleted = computed(() => 
    this.status() === 'Succeeded' || this.status() === 'Failed'
  );

  ngOnInit(): void {
    const id = this.route.snapshot.paramMap.get('runId');
    if (id) {
      this.runId.set(id);
      this.fetchRun();

      this.pollInterval = setInterval(() => {
        if (!this.isCompleted()) {
          this.fetchRun();
        } else {
          clearInterval(this.pollInterval);
        }
      }, 1000);
    }
  }

  fetchRun(): void {
    this.api.getRunStatus(this.runId()).subscribe({
      next: (data) => {
        this.run.set(data);
        this.isLoading.set(false);

        // Fetch the pipeline blueprint if we haven't yet
        if (this.pipelineSteps().length === 0 && data.pipelineId) {
          this.api.getPipelineById(data.pipelineId).subscribe(p => {
            this.pipelineSteps.set(p.steps);
          });
        }
      },
      error: (err) => {
        console.error('Failed to fetch run:', err);
        this.errorMessage.set('Failed to load execution details.');
        this.isLoading.set(false);
      }
    });
  }

  ngOnDestroy(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
    }
  }
}