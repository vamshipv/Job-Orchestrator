import { 
  Component, 
  ElementRef, 
  Input, 
  OnChanges, 
  OnDestroy, 
  AfterViewInit, 
  ViewChild, 
  SimpleChanges 
} from '@angular/core';
import React from 'react';
import ReactDOM from 'react-dom/client';
import { ReactDagCanvas } from './dag-canvas';
import { PipelineStepDefinition, StepExecutionStatus } from '../models/pipeline.model';

@Component({
  selector: 'app-react-dag-canvas',
  standalone: true,
  template: `<div #container class="react-dag-wrapper"></div>`,
  styles: [`
    .react-dag-wrapper {
      width: 100%;
      height: 360px;
      margin-bottom: 1.5rem;
    }
  `]
})
export class ReactDagCanvasComponent implements AfterViewInit, OnChanges, OnDestroy {
  @ViewChild('container', { static: true }) containerRef!: ElementRef<HTMLDivElement>;

  @Input() steps: PipelineStepDefinition[] = [];
  @Input() statuses: StepExecutionStatus[] = [];

  private root: ReactDOM.Root | null = null;

  ngAfterViewInit(): void {
    // 1. Create the React Root on the Angular-managed DOM element
    this.root = ReactDOM.createRoot(this.containerRef.nativeElement);
    this.renderReact();
  }

  ngOnChanges(changes: SimpleChanges): void {
    // 2. Whenever Angular @Inputs change (steps or statuses update), re-render the React canvas
    if (this.root) {
      this.renderReact();
    }
  }

  private renderReact(): void {
    if (!this.root) return;

    // 3. Render the React component inside the Angular view
    this.root.render(
      React.createElement(ReactDagCanvas, {
        steps: this.steps,
        statuses: this.statuses
      })
    );
  }

  ngOnDestroy(): void {
    // 4. Memory Leak Protection: Always unmount the React root when Angular destroys this component!
    if (this.root) {
      this.root.unmount();
      this.root = null;
    }
  }
}