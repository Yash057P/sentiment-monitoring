import { useNavigate, useLocation } from 'react-router-dom';
import { LogOut, Sun, Moon, LayoutDashboard, ShieldCheck, Info, Settings as SettingsIcon, LogIn } from 'lucide-react';
import { clearSession, getProfile } from '../api.js';
import { useT } from '../i18n.jsx';
import { useTheme } from '../theme.jsx';
import { companies } from './companies.js';

export function useProfile() {
  return getProfile();
}

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const next = theme === 'dark' ? 'light' : 'dark';
  return (
    <button className="icon-btn hide-sm" onClick={() => setTheme(next)} title="Toggle theme">
      {theme === 'dark' ? <Sun size={18} /> : <Moon size={18} />}
    </button>
  );
}

/* Desktop taskbar: the same items the mobile drawer carries. */
export function NavLinks() {
  const { t } = useT();
  const navigate = useNavigate();
  const location = useLocation();
  const profile = getProfile();

  const items = profile
    ? [
        {
          to: profile.role === 'admin' ? '/admin' : '/company',
          icon: profile.role === 'admin' ? ShieldCheck : LayoutDashboard,
          label: profile.role === 'admin' ? t('nav.admin') : t('nav.dashboard'),
        },
        { to: '/about', icon: Info, label: t('nav.about') },
        { to: '/settings', icon: SettingsIcon, label: t('nav.settings') },
      ]
    : [
        { to: '/login', icon: LogIn, label: t('nav.login') },
        { to: '/about', icon: Info, label: t('nav.about') },
        { to: '/settings', icon: SettingsIcon, label: t('nav.settings') },
      ];

  return (
    <nav className="nav-links hide-sm">
      {items.map((item) => {
        const Icon = item.icon;
        const active = location.pathname === item.to;
        return (
          <button
            key={item.to}
            className={`nav-link ${active ? 'active' : ''}`}
            onClick={() => navigate(item.to)}
          >
            <Icon size={16} />
            {item.label}
          </button>
        );
      })}
    </nav>
  );
}

export function TopBar({ companyColor, companyLogo, title, subtitle }) {
  const navigate = useNavigate();
  const { t } = useT();
  const profile = getProfile();

  function logout() {
    clearSession();
    navigate('/login', { replace: true });
  }

  return (
    <header className="topbar">
      <div className="topbar-inner">
        <div
          className="brand"
          onClick={() => navigate(profile?.role === 'admin' ? '/admin' : '/company')}
        >
          <span className="logo-badge">{title === 'Admin' ? '🛡️' : companyLogo || '📊'}</span>
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            <span>
              {title === 'Admin' ? 'BrandScope Admin' : title || 'BrandScope'}
            </span>
            {subtitle && (
              <span className="text-muted" style={{ fontSize: 12, fontWeight: 500 }}>
                {subtitle}
              </span>
            )}
          </div>
        </div>
        <div className="spacer" />
        <NavLinks />
        <div className="spacer" />
        <ThemeToggle />
        <span className="text-muted hide-sm" style={{ fontSize: 14 }}>
          {profile?.display || profile?.username}
        </span>
        {profile && (
          <button className="btn ghost sm hide-sm" onClick={logout}>
            <LogOut size={16} /> {t('logout')}
          </button>
        )}
      </div>
    </header>
  );
}

export function CompanyChip({ handle, big = false }) {
  const c = companies.find((x) => x.handle === handle);
  if (!c) return <span className="text-muted">—</span>;
  return (
    <span className="row" style={{ gap: 8 }}>
      <span
        className="logo-cell"
        style={{ background: c.color, width: big ? 44 : 30, height: big ? 44 : 30, borderRadius: big ? 12 : 8 }}
      >
        {c.logo}
      </span>
      <span style={{ fontWeight: big ? 700 : 600 }}>{c.name}</span>
    </span>
  );
}

export function StatusBadge({ outlook }) {
  const value = String(outlook || 'unknown').toLowerCase();
  const cls = value === 'good' ? 'good' : value === 'bad' ? 'bad' : 'warn';
  const label = String(outlook || 'Awaiting data');
  return <span className={`badge dot ${cls}`}>{label}</span>;
}

export function SentimentBadge({ sentiment }) {
  const s = String(sentiment || '').toLowerCase();
  const cls = s === 'positive' ? 'good' : s === 'negative' ? 'bad' : 'warn';
  return <span className={`badge ${cls}`}>{sentiment || '—'}</span>;
}

export function Kpi({ label, value, sub, tone }) {
  return (
    <div className={`kpi ${tone || ''}`}>
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {sub && <span className="sub">{sub}</span>}
    </div>
  );
}

export function Empty({ title, hint }) {
  return (
    <div className="empty">
      <div className="big">📡</div>
      <b>{title}</b>
      <div>{hint}</div>
    </div>
  );
}

export function MiniBar({ pct, color = '#4f46e5' }) {
  return (
    <div
      style={{
        width: 80,
        height: 8,
        background: '#e2e8f0',
        borderRadius: 999,
        overflow: 'hidden',
      }}
    >
      <div
        style={{ width: `${Math.min(100, Math.max(0, pct * 100))}%`, height: '100%', background: color, borderRadius: 999 }}
      />
    </div>
  );
}