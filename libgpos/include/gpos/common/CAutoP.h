//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename: 
//		CAutoP.h
//
//	@doc:
//		Basic auto pointer implementation; do not anticipate ownership based
//		on assignment to other auto pointers etc. Require explicit return/assignment
//		to re-init the object;
//
//		This is primarily used for auto destruction.
//		Not using Boost's auto pointer logic to discourage blurring ownership
//		of objects.
//
//	@owner: 
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoP_H
#define GPOS_CAutoP_H

#include <type_traits>

#include "gpos/base.h"
#include "gpos/common/CAutoPointerBase.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoP
	//
	//	@doc:
	//		Wrapps pointer of type T; overloads *, ->, = does not provide
	//		copy ctor;
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CAutoP : public CAutoPointerBase<T>
	{
		private:
			static_assert(!std::is_base_of<CRefCount, T>::value, "T must not be a CRefCount");

			typedef CAutoPointerBase<T> _base;

		protected:
						
			// hidden copy ctor
			CAutoP<T>
				(
				const CAutoP&
				);

		public:
		
			// ctor
			explicit
			CAutoP<T>()
				:
					_base(NULL)
			{}

			explicit
			CAutoP<T>(T *pt)
				:
					_base(pt)
			{}

			CAutoP const& operator = (T* pt)
			{
				_base::m_pt = pt;
				return *this;
			}

			// dtor
			virtual ~CAutoP();
	}; // class CAutoP

	//---------------------------------------------------------------------------
	//	@function:
	//		CAutoP::~CAutoP
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template <class T>
	CAutoP<T>::~CAutoP()
	{
		GPOS_DELETE(_base::m_pt);
	}
}


#endif // !GPOS_CAutoP_H

// EOF

