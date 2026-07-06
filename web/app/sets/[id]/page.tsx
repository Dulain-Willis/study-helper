import Link from "next/link";
import { notFound } from "next/navigation";
import { prisma } from "@/lib/db";
import { addCard } from "@/app/actions";
import { CardList } from "@/app/components/card-list";

export default async function SetPage(props: PageProps<"/sets/[id]">) {
  const { id } = await props.params;

  const set = await prisma.set.findUnique({
    where: { id },
    include: {
      group: true,
      cards: { orderBy: { createdAt: "asc" } },
    },
  });

  if (!set) notFound();

  return (
    <div className="space-y-8">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-zinc-500 dark:text-zinc-400">
        <Link href="/" className="hover:text-zinc-900 dark:hover:text-zinc-100">Library</Link>
        <span>/</span>
        <Link href={`/groups/${set.group.id}`} className="hover:text-zinc-900 dark:hover:text-zinc-100">
          {set.group.name}
        </Link>
        <span>/</span>
        <span className="text-zinc-900 dark:text-zinc-100 font-medium">{set.name}</span>
      </nav>

      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">{set.name}</h1>
        {set.cards.length > 0 && (
          <div className="flex gap-2">
            <Link
              href={`/sets/${id}/study`}
              className="rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-2 text-sm font-medium hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
            >
              Browse
            </Link>
            <Link
              href={`/sets/${id}/study?mode=practice`}
              className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
            >
              Practice
            </Link>
          </div>
        )}
      </div>

      <p className="text-zinc-500 dark:text-zinc-400 text-sm">{set.cards.length} card{set.cards.length !== 1 ? "s" : ""}</p>

      {/* Cards */}
      {set.cards.length === 0 ? (
        <p className="text-zinc-400 dark:text-zinc-500 text-sm">No cards yet. Add one below.</p>
      ) : (
        <CardList cards={set.cards} />
      )}

      {/* Add card */}
      <section className="border-t border-zinc-200 dark:border-zinc-800 pt-6 space-y-3">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">Add Card</h2>
        <form action={addCard} className="space-y-3">
          <input type="hidden" name="setId" value={id} />
          <textarea
            name="front"
            required
            placeholder="Front (question)"
            rows={2}
            className="w-full rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-zinc-400 resize-none"
          />
          <textarea
            name="back"
            required
            placeholder="Back (answer)"
            rows={3}
            className="w-full rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-zinc-400 resize-none"
          />
          <button
            type="submit"
            className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
          >
            Add Card
          </button>
        </form>
      </section>
    </div>
  );
}
