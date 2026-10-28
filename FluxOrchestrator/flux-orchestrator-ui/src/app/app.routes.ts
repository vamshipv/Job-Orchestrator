import { Routes } from '@angular/router';
import { PipelineCatalogComponent } from './components/pipeline-catalog/pipeline-catalog';
import { RunDetailComponent } from './components/run-detail/run-detail';

export const routes: Routes = [
  { path: '', redirectTo: 'pipelines', pathMatch: 'full' },
  { path: 'pipelines', component: PipelineCatalogComponent },
  { path: 'runs/:runId', component: RunDetailComponent },
  { path: '**', redirectTo: 'pipelines' }
];