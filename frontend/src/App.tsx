import { useMemo, useState } from "react";
import { Route, Routes } from "react-router-dom";
import { TopNav } from "./components/TopNav";
import { HomePage } from "./pages/HomePage";
import { ProductPage } from "./pages/ProductPage";
import { SearchPage } from "./pages/SearchPage";
import { UserIdContext, readStoredUserId, storeUserId } from "./user";

export function App() {
  const [userId, setUserIdState] = useState(readStoredUserId);
  const value = useMemo(
    () => ({
      userId,
      setUserId: (next: string) => {
        storeUserId(next);
        setUserIdState(next);
      },
    }),
    [userId]
  );

  return (
    <UserIdContext.Provider value={value}>
      <div className="app">
        <TopNav />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/search" element={<SearchPage />} />
            <Route path="/product/:id" element={<ProductPage />} />
          </Routes>
        </main>
        <footer className="footer">
          <p>On-Premise Recommender System</p>
        </footer>
      </div>
    </UserIdContext.Provider>
  );
}
