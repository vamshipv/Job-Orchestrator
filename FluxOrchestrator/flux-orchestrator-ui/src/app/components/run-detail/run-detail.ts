import { Component, inject, OnInit, signal, computed, OnDestroy } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { DatePipe } from '@angular/common';
import { PipelineApiService } from '../../services/pipeline-api.service';
import { PipelineRun } from '../../models/pipeline.model';

@Component({
  selector: 'app-run-detail',
  standalone: true,
  imports: [RouterLink, DatePipe],
  templateUrl: './run-detail.html',
  styleUrl: './run-detail.scss'
})
export class RunDetailComponent implements OnInit, OnDestroy {
  private route = inject(ActivatedRoute);
  private api = inject(PipelineApiService);

  // --- Angular Signals for Reactive State ---
  runId = signal<string>('');
  run = signal<PipelineRun | null>(null);
  isLoading = signal<boolean>(true);
  errorMessage = signal<string | null>(null);

  private pollInterval: any = null;

  // --- Computed Signals (Derived State) ---
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

      // Poll every 1s until SignalR is wired on Day 7
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