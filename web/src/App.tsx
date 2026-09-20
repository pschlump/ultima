// App shell: left-nav layout with connection/user indicator and logout,
// react-router routes per design doc §10.2. Admin nav is hidden for
// non-admin accounts; on auth-disabled servers the Admin screen itself
// shows a notice.
import { NavLink, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { useEffect } from "react";
import { useAuth } from "./lib/auth";
import { Login } from "./screens/Login";
import { Dashboard } from "./screens/Dashboard";
import { Keys } from "./screens/Keys";
import { Console } from "./screens/Console";
import { Monitor } from "./screens/Monitor";
import { Slowlog } from "./screens/Slowlog";
import { Config } from "./screens/Config";
import { PubSub } from "./screens/PubSub";
import { Admin } from "./screens/Admin";

const NAV = [
  { to: "/", label: "Dashboard", end: true },
  { to: "/keys", label: "Keys", end: false },
  { to: "/console", label: "Console", end: false },
  { to: "/monitor", label: "Monitor", end: false },
  { to: "/slowlog", label: "Slowlog", end: false },
  { to: "/config", label: "Config", end: false },
  { to: "/pubsub", label: "Pub/Sub", end: false },
];

function Shell() {
  const { session, logout } = useAuth();
  const navigate = useNavigate();
  const isAdmin = session?.accountClass === "admin" || (session?.authDisabled ?? false);

  const onLogout = () => {
    void logout().then(() => navigate("/"));
  };

  return (
    <div className="shell">
      <nav className="sidenav">
        <div className="brand">Ultima</div>
        {NAV.map((n) => (
          <NavLink key={n.to} to={n.to} end={n.end} className={({ isActive }) => (isActive ? "nav-link nav-active" : "nav-link")}>
            {n.label}
          </NavLink>
        ))}
        {isAdmin && (
          <NavLink to="/admin" className={({ isActive }) => (isActive ? "nav-link nav-active" : "nav-link")}>
            Admin
          </NavLink>
        )}
        <div className="spacer" />
        <div className="conn-info">
          {session?.authDisabled ? (
            <span className="muted">auth disabled</span>
          ) : (
            <>
              <span className="mono">{session?.username}</span>
              <span className="muted"> ({session?.accountClass})</span>
            </>
          )}
        </div>
        {!session?.authDisabled && (
          <button className="nav-logout" onClick={onLogout}>
            Log out
          </button>
        )}
      </nav>
      <main className="main">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/keys" element={<Keys />} />
          <Route path="/console" element={<Console />} />
          <Route path="/monitor" element={<Monitor />} />
          <Route path="/slowlog" element={<Slowlog />} />
          <Route path="/config" element={<Config />} />
          <Route path="/pubsub" element={<PubSub />} />
          <Route path="/admin" element={<Admin />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

export default function App() {
  const { ready, session } = useAuth();

  // When the session is dropped mid-use (refresh failure), react to it.
  const navigate = useNavigate();
  useEffect(() => {
    if (ready && !session) navigate("/");
  }, [ready, session, navigate]);

  if (!ready) {
    return (
      <div className="login-page">
        <p className="muted">Connecting…</p>
      </div>
    );
  }
  if (!session) return <Login />;
  return <Shell />;
}
