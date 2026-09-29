import { useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { getProfile, clearSession } from './api.js';
import LoginPage from './pages/LoginPage.jsx';
import AdminDashboard from './pages/AdminDashboard.jsx';
import CompanyDashboard from './pages/CompanyDashboard.jsx';
import AboutPage from './pages/AboutPage.jsx';
import SettingsPage from './pages/SettingsPage.jsx';

export default function App() {
  const navigate = useNavigate();
  const location = useLocation();
  const profile = getProfile();
  const path = location.pathname;

  useEffect(() => {
    if (!profile) clearSession();
  }, [profile]);

  const isPublicPage = path.startsWith('/about') || path.startsWith('/settings');

  if (!profile) {
    if (isPublicPage) {
      return path.startsWith('/about') ? <AboutPage /> : <SettingsPage />;
    }
    return (
      <LoginPage
        onLogin={(data) => navigate(data.role === 'admin' ? '/admin' : '/company', { replace: true })}
      />
    );
  }

  if (path.startsWith('/about')) return <AboutPage />;
  if (path.startsWith('/settings')) return <SettingsPage />;
  if (path.startsWith('/admin') && profile.role === 'admin') return <AdminDashboard />;
  if (path.startsWith('/company') && profile.role === 'company') return <CompanyDashboard />;
  return profile.role === 'admin' ? <AdminDashboard /> : <CompanyDashboard />;
}