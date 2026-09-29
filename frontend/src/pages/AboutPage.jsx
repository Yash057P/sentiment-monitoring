import { useNavigate } from 'react-router-dom';
import { Activity, Cpu, Rss, TrendingUp, Building2, ShieldCheck, GraduationCap, FlaskConical, Megaphone, FolderKanban, ArrowRight } from 'lucide-react';
import { useT } from '../i18n.jsx';
import Drawer from '../components/Drawer.jsx';
import { TopBar } from '../components/ui.jsx';

const steps = [
  { icon: Rss, key: 'how1' },
  { icon: Cpu, key: 'how2' },
  { icon: Activity, key: 'how3' },
  { icon: TrendingUp, key: 'how4' },
];
const who = [
  { icon: Building2, key: 'who1' },
  { icon: ShieldCheck, key: 'who2' },
  { icon: GraduationCap, key: 'who3' },
];
const where = [
  { icon: FlaskConical, key: 'where1' },
  { icon: Megaphone, key: 'where2' },
  { icon: FolderKanban, key: 'where3' },
];

export default function AboutPage() {
  const { t } = useT();
  const navigate = useNavigate();

  return (
    <div className="about-page">
      <Drawer />
      <TopBar title="BrandScope" subtitle={t('about.eyebrow')} />

      <main className="container about">
        <div className="about-hero">
          <span className="eyebrow">{t('about.eyebrow')}</span>
          <h1>{t('about.title')}</h1>
          <p className="lede">{t('about.lede')}</p>
        </div>

        <div className="card">
          <h3>{t('about.howTitle')}</h3>
          <div className="grid steps">
            {steps.map((s, i) => {
              const Icon = s.icon;
              return (
                <div className="step" key={s.key}>
                  <span className="step-n">{i + 1}</span>
                  <span className="step-icon"><Icon size={22} /></span>
                  <b>{t(`about.${s.key}t`)}</b>
                  <p>{t(`about.${s.key}d`)}</p>
                </div>
              );
            })}
          </div>
        </div>

        <div className="grid two-col">
          <div className="card">
            <h3>{t('about.whoTitle')}</h3>
            {who.map((w) => {
              const Icon = w.icon;
              return (
                <div className="feature-row" key={w.key}>
                  <span className="step-icon"><Icon size={20} /></span>
                  <div>
                    <b>{t(`about.${w.key}t`)}</b>
                    <p className="text-muted">{t(`about.${w.key}d`)}</p>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="card">
            <h3>{t('about.whereTitle')}</h3>
            {where.map((w) => {
              const Icon = w.icon;
              return (
                <div className="feature-row" key={w.key}>
                  <span className="step-icon"><Icon size={20} /></span>
                  <div>
                    <b>{t(`about.${w.key}t`)}</b>
                    <p className="text-muted">{t(`about.${w.key}d`)}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="cta-strip">
          <p>{t('about.cta')}</p>
          <button className="btn" onClick={() => navigate('/login')}>
            {t('nav.login')} <ArrowRight size={16} />
          </button>
        </div>
      </main>
    </div>
  );
}