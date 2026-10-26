import { Routes } from '@angular/router';
import { PipelineCatalogComponent } from './components/pipeline-catalog/pipeline-catalog';

export const routes: Routes = [
  { path: '', redirectTo: 'pipelines', pathMatch: 'full' },
  { path: 'pipelines', component: PipelineCatalogComponent },
  { path: '**', redirectTo: 'pipelines' }
];