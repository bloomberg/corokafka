/*
** Copyright 2019 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
#ifndef BLOOMBERG_COROKAFKA_INTERVAL_SET_H
#define BLOOMBERG_COROKAFKA_INTERVAL_SET_H

#include <algorithm>
#include <map>
#include <ostream>
#include <type_traits>

namespace Bloomberg {
namespace corokafka {

template<typename T>
class Range : public std::pair<T,T>
{
public:
    Range();
    Range(T first, T second);
    Range(std::pair<T,T> pair);
    Range(std::pair<const T,T> pair);

    template<typename U>
    friend std::ostream &
    operator<<(std::ostream   &out,
               Range<U> const &i);
};

template<typename T>
class Point : public Range<T>
{
public:
    Point(T Point);
};

template<
    class T,
    class Compare = std::less<T>,
    class Alloc   = std::allocator<std::pair<T,T> > >
class IntervalSet : public std::map<T,T,Compare,Alloc>
{
public:
    template<typename U>
    friend std::ostream& operator<<(std::ostream &out, IntervalSet<U> const &is);
    typedef std::map<T,T,Compare,Alloc> MapType;
    typedef typename MapType::iterator Iterator;
    typedef typename MapType::const_iterator ConstIterator;
    typedef std::pair<Iterator,bool> InsertReturnType;
    using MapType::MapType;
    using MapType::erase;

    // Methods
    /**
     * @brief Inserts a range or a point.
     * @param interval The range or point.
     * @return A pair containing an iterator to the newly inserted or modified range and a boolean indicating
     *         if the map was modified or not. The only case where the map is *not* modified is when a range or
     *         point is completely absorbed by an existing larger range.
     */
    InsertReturnType insert(const Range<T>& interval);
#if 0
    size_t erase(const Range<T>& interval);
#endif
    /**
     * @brief Return the number of elements contained by all ranges of the map.
     * @return The number of elements.
     * @warning Complexity is O(N).
     */
    size_t count() const;

private:
    bool canAbsorb(const Iterator& left, const Iterator& right);
    bool canMerge(const Iterator& left, const Iterator& right);
};

/* =============================== IMPLEMENTATIONS ========================================== */

template<typename T>
bool operator==(const std::pair<const T,T>& pair, const Range<T>& point) {
    return pair.first == point.first && pair.second == point.second;
}

template<typename T>
bool operator==(const Range<T>& point, const std::pair<const T,T>& pair) {
    return pair.first == point.first && pair.second == point.second;
}

template<typename T>
Range<T>::Range()
{
    static_assert(std::is_integral<T>::value, "Integral type required.");
}

template<typename T>
Range<T>::Range( const T first, const T second )
    : std::pair<T, T>( first, second )
{
    static_assert(std::is_integral<T>::value, "Integral type required.");
}

template<typename T>
Range<T>::Range( std::pair<T, T> pair )
    : std::pair<T, T>( pair )
{
    static_assert(std::is_integral<T>::value, "Integral type required.");
}

template<typename T>
Range<T>::Range( std::pair<const T, T> pair )
{
    this->first  = pair.first;
    this->second = pair.second;
    static_assert(std::is_integral<T>::value, "Integral type required.");
}

template<typename T>
Point<T>::Point( const T Point )
    : Range<T>( Point, Point )
{
}

template<class T, class Compare, class Alloc>
bool IntervalSet<T,Compare,Alloc>::canAbsorb(const Iterator& left, const Iterator& right)
{
    if (left == this->end() || right == this->end()) {
        return false;
    }
    return (left->first <= right->first) &&
           (left->second >= right->second);
}

template<class T, class Compare, class Alloc>
bool IntervalSet<T,Compare,Alloc>::canMerge(const Iterator& left, const Iterator& right)
{
    if (left == this->end() || right == this->end()) {
        return false;
    }
    return ((left->first <= right->first) &&
           (((left->second >= right->first) && (left->second < right->second)) || (left->second+1 == right->first)));
}

template<class T, class Compare, class Alloc>
typename IntervalSet<T,Compare,Alloc>::InsertReturnType
IntervalSet<T,Compare,Alloc>::insert(const Range<T>& interval)
{
    Iterator current(this->end()),
             upper(this->upper_bound(interval.second)),
             next(this->end()),
             it(this->end()); //it = iterator to be returned from this function

    if (upper != this->end()) ++upper;
    bool isModified = true;
    const InsertReturnType irt = MapType::insert(interval);
    current = it = irt.first;

    if (irt.second) {
        //new insertion
        if (it != this->begin()) {
            // Get the previous element in the map
            current = --Iterator(it);
            isModified = !canAbsorb(current, it);
            if (canAbsorb(current, it) || canMerge(current, it)) {
                it = current; //reset it to point to the previous element
            }
        }
    }
    else {
        //value exists so we merge it with the current range
        if (interval.second <= it->second) {
            isModified = false;
        }
        it->second = std::max(interval.second, it->second);
    }

    //Process ranges
    next = current; ++next;
    while (next != upper) {
        if (canAbsorb(current, next)) {
            MapType::erase(next++);
        }
        else if (canMerge(current, next)) {
            current->second = next->second;
            MapType::erase(next++);
        }
        else {
            ++next; ++current;
        }
    }

    return std::make_pair(it, isModified);
}

#if 0
template<class T, class Compare, class Alloc>
size_t
IntervalSet<T,Compare,Alloc>::erase(const Range<T>& interval)
{
    if (this->empty()) {
        return 0;
    }

    /* current element */
    typename IntervalSet::iterator current = this->begin();

    /* next element in map whose key is > than new interval min */
    typename IntervalSet::iterator next = this->upper_bound( interval.first );

    if ( next != this->begin() )
    {
        /* since next != begin(), --next is a valid iterator */
        typename IntervalSet::iterator prev = next;
        --prev;

        if ( prev->second < interval.first )
            current = next;
        else
            current = prev;
    }

    while ( current != this->end() && current->first <= interval.second )
    {
        if ( current->first >= interval.first &&
             current->second <= interval.second )
        {
            current = std::map<T, T>::erase( current );
        }
        else if ( interval.second >= current->second )
        {
            current->second =
                std::min( current->second - 1, interval.first - 1 );
            current++;
        }
        else
        {
            int second = current->second;
            if ( interval.first <= current->first )
                current =
                    std::map<T, T>::erase( current );
            else
                current->second = interval.first - 1;

            current = std::map<T, T>::insert(
                current, Range<T>( interval.second + 1, second ) );
        }
    }

    return 0;
}
#endif

template<class T, class Compare, class Alloc>
size_t
IntervalSet<T,Compare,Alloc>::count() const
{
    size_t count = 0;
    for (typename IntervalSet<T,Compare,Alloc>::ConstIterator it = this->begin(); it != this->end(); it++) {
        count += (it->second-it->first) + 1;
    }
    return count;
}

template<class T>
std::ostream &
operator<<(std::ostream &out, const Range<T>& r)
{
    if (r.first == r.second)
        out << "{" << r.first << "}";
    else
        out << "{" << r.first << "-" << r.second << "}";
    return out;
}

template<class T>
std::ostream &
operator<<(std::ostream &out, IntervalSet<T> const &is)
{
    out << "{";
    for (typename IntervalSet<T>::ConstIterator it = is.begin(); it != is.end(); it++) {
        out << Range<T>(*it);
    }
    out << "}";
    return out;
}

}  // namespace corokafka
}  // namespace Bloomberg

#endif  // BLOOMBERG_COROKAFKA_INTERVAL_SET_H
