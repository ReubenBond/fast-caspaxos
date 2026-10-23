import { createRoot } from 'react-dom/client';
import { App } from './App';
import { ClusterStore } from './store';
import './style.css';

// Vite development targets an explicitly trusted backend origin; production uses its serving host.
const origin = import.meta.env.VITE_API_ORIGIN || window.location.origin;
createRoot(document.getElementById('root')!).render(<App store={new ClusterStore(origin)} />);
