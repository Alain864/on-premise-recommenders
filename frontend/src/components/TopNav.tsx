import { Link } from "react-router-dom";
import { SearchBar } from "./SearchBar";
import { useUserId } from "../user";

export function TopNav() {
  const { userId, setUserId } = useUserId();
  return (
    <nav className="top-nav">
      <div className="nav-content">
        <Link to="/" className="logo">
          Recommender
        </Link>
        <SearchBar />
        <label className="user-control">
          User ID
          <input
            value={userId}
            placeholder="anonymous"
            onChange={(event) => setUserId(event.target.value.trim())}
          />
        </label>
      </div>
    </nav>
  );
}
